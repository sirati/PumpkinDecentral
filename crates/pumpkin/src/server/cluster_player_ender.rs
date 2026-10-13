use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::replica_inventory::PlayerEnderSnapshot;
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

struct SentHead {
    snapshot: Arc<PlayerEnderSnapshot>,
    publication_generation: Option<u64>,
    recipients: BTreeSet<u16>,
}

pub(super) struct OwnedPlayerEnderSnapshots {
    sequence: u64,
    next_player: usize,
    heads: BTreeMap<GlobalPlayerId, SentHead>,
    presentations: Vec<Arc<PlayerEnderSnapshot>>,
    diagnostics: BTreeMap<GlobalPlayerId, std::time::Instant>,
}
impl OwnedPlayerEnderSnapshots {
    pub(super) fn new() -> Self {
        Self {
            sequence: 0,
            next_player: 0,
            heads: BTreeMap::new(),
            presentations: Vec::new(),
            diagnostics: BTreeMap::new(),
        }
    }
    pub(super) fn take_presentations(&mut self) -> Vec<Arc<PlayerEnderSnapshot>> {
        std::mem::take(&mut self.presentations)
    }
    pub(super) fn emit(
        &mut self,
        server: &super::Server,
        actors: &super::cluster_world_actor::OwnedActors,
        players: &[Arc<crate::entity::player::Player>],
        queue: &mut VecDeque<OutboundParcel>,
        tick: pumpkin_cluster::time::TickStamp,
    ) {
        let generations = server.cluster_peer_incarnations.load();
        self.heads.retain(|gid, head| {
            generations.get(&head.snapshot.source.server) == Some(&head.snapshot.source.generation)
                && actors.local_key(*gid).is_some()
                && actors
                    .player_client_identity(*gid, head.snapshot.source)
                    .is_some_and(|(lease, _)| lease == head.snapshot.lease)
        });
        let directory = server.cluster_chunk_directory.load();
        let mut queued = queue.iter().map(|parcel| parcel.bytes.len()).sum::<usize>();
        let start = self.next_player % players.len().max(1);
        for step in 0..players.len() {
            let index = (start + step) % players.len();
            self.next_player = (index + 1) % players.len();
            let player = &players[index];
            let Some(lease) = super::cluster_world_owner::player_action_lease(server, player)
            else {
                continue;
            };
            let Some(generation) = generations.get(&lease.gid.server).copied() else {
                continue;
            };
            let source = PeerGeneration {
                server: lease.gid.server,
                generation,
            };
            if actors.local_key(lease.gid) != Some(lease.key.0)
                || actors
                    .player_client_identity(lease.gid, source)
                    .is_none_or(|(actual, _)| actual != lease)
            {
                continue;
            }
            let install_generation = player
                .cluster_owner_install_generation
                .load(std::sync::atomic::Ordering::Acquire);
            if install_generation == 0 {
                continue;
            }
            let Some(spatial) = actors.phase_spatial(lease.gid, false) else {
                continue;
            };
            let mut recipients = directory
                .sorted_holders(&spatial.chunk)
                .into_iter()
                .collect::<BTreeSet<_>>();
            recipients.extend(
                directory
                    .subscriptions_at(spatial.chunk, tick)
                    .into_iter()
                    .map(|entry| entry.subscriber),
            );
            recipients.remove(&lease.gid.server.0);
            let observed = super::cluster_invsee::published_ender_inventory_revision_for_player(
                server, player, lease,
            );
            let publication_generation = observed.map(|(generation, _)| generation);
            let revision = observed.map(|(_, revision)| revision);
            let previous = self
                .heads
                .get(&lease.gid)
                .filter(|head| head.snapshot.matches(lease, source, install_generation));
            if previous.is_some_and(|head| {
                head.publication_generation == publication_generation
                    && head.snapshot.revision == revision
                    && head.recipients == recipients
            }) {
                continue;
            }
            let frozen = if previous.is_some_and(|head| {
                head.publication_generation == publication_generation
                    && head.snapshot.revision == revision
            }) {
                previous.map(|head| {
                    (
                        head.publication_generation,
                        head.snapshot.revision,
                        head.snapshot.cells.clone(),
                    )
                })
            } else {
                Some(
                    match super::cluster_invsee::published_ender_inventory_for_player(
                        server, player, lease,
                    ) {
                        Some(frozen) => (
                            Some(frozen.publication_generation),
                            Some(frozen.revision),
                            Some(Box::new(frozen.cells)),
                        ),
                        None => (None, None, None),
                    },
                )
            };
            let Some((publication_generation, revision, cells)) = frozen else {
                continue;
            };
            let snapshot = PlayerEnderSnapshot {
                lease,
                source,
                install_generation,
                sequence: self.sequence.wrapping_add(1),
                revision,
                cells,
            };
            let Some(encoded) = snapshot.encode() else {
                if self
                    .diagnostics
                    .get(&lease.gid)
                    .is_none_or(|last| last.elapsed().as_secs() >= 300)
                {
                    tracing::error!(
                        ?lease,
                        "canonical ender projection exceeds accepted inventory bounds"
                    );
                    self.diagnostics
                        .insert(lease.gid, std::time::Instant::now());
                }
                continue;
            };
            let bytes = encoded
                .len()
                .checked_mul(recipients.len())
                .and_then(|bytes| bytes.checked_add(queued));
            if bytes.is_none_or(|bytes| {
                bytes > pumpkin_cluster::canonical_action::MAX_LOGICAL_ACTION_BYTES
            }) || queue
                .len()
                .checked_add(recipients.len())
                .is_none_or(|count| count > 65535)
            {
                self.next_player = index;
                break;
            }
            self.sequence = snapshot.sequence;
            queued = bytes.unwrap();
            let snapshot = Arc::new(snapshot);
            for peer in &recipients {
                queue.push_back(OutboundParcel {
                    peer: pumpkin_cluster::identity::ServerId(*peer),
                    header: StreamHeader::new(
                        pumpkin_cluster::protocol::StreamKind::PlayerVisual,
                        Some(lease.gid),
                    ),
                    bytes: encoded.clone(),
                });
            }
            self.presentations.push(snapshot.clone());
            self.heads.insert(
                lease.gid,
                SentHead {
                    snapshot,
                    publication_generation,
                    recipients,
                },
            );
        }
    }
}

pub(crate) fn write_cells_nbt(
    nbt: &mut pumpkin_nbt::compound::NbtCompound,
    cells: &[pumpkin_cluster::inventory::InventoryCell; 27],
) -> Option<()> {
    let mut items = Vec::new();
    for (slot, cell) in cells.iter().enumerate() {
        if cell.loc
            != pumpkin_cluster::inventory::InvLoc::new(
                pumpkin_cluster::inventory::INV_MAIN,
                slot as u16,
            )
        {
            return None;
        }
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell).ok()?;
        if stack.is_empty() {
            continue;
        }
        let mut item = pumpkin_nbt::compound::NbtCompound::new();
        stack.write_item_stack(&mut item);
        item.put_byte("Slot", slot as i8);
        items.push(pumpkin_nbt::tag::NbtTag::Compound(item));
    }
    nbt.put("EnderItems", pumpkin_nbt::tag::NbtTag::List(items));
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::inventory::{INV_MAIN, InvLoc, InventoryCell};
    use pumpkin_data::item_stack::ItemStack;

    #[test]
    fn frozen_ender_image_exports_actual_last_slot_and_failed_replacement_is_atomic() {
        let mut cells =
            std::array::from_fn(|slot| InventoryCell::empty(InvLoc::new(INV_MAIN, slot as u16)));
        cells[26] = super::super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, 26),
            &ItemStack::new(17, &pumpkin_data::item::Item::DIAMOND),
        )
        .unwrap();
        let frozen = Arc::new(cells.clone());
        let mut nbt = pumpkin_nbt::compound::NbtCompound::new();
        assert_eq!(write_cells_nbt(&mut nbt, &frozen), Some(()));
        let stacks = super::super::cluster_invsee::ender_chest_stacks_from_nbt(Some(&nbt));
        assert!(stacks[..26].iter().all(ItemStack::is_empty));
        assert_eq!(stacks[26].item.id, pumpkin_data::item::Item::DIAMOND.id);
        assert_eq!(stacks[26].item_count, 17);
        let before = nbt.clone();
        cells[26].nbt.push(0);
        assert_eq!(write_cells_nbt(&mut nbt, &cells), None);
        assert_eq!(nbt, before);
        assert_eq!(frozen[26].count, 17);
        cells[26] = InventoryCell::empty(InvLoc::new(INV_MAIN, 26));
        assert_eq!(write_cells_nbt(&mut nbt, &cells), Some(()));
        assert!(
            super::super::cluster_invsee::ender_chest_stacks_from_nbt(Some(&nbt))
                .iter()
                .all(ItemStack::is_empty)
        );
        assert_eq!(frozen[26].count, 17);
    }
}

pub(crate) fn matches_canonical_root(
    server: &super::Server,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    cells: &[pumpkin_cluster::inventory::InventoryCell; 27],
) -> bool {
    super::cluster_invsee::published_canonical_ender_root(server, lease)
        .is_some_and(|(_, root)| matches_root_cells(&root, cells))
}
fn matches_root_cells(
    root: &pumpkin_cluster::inventory::InventoryStateSnapshot,
    cells: &[pumpkin_cluster::inventory::InventoryCell; 27],
) -> bool {
    if root.cells.len() != 27 {
        return false;
    }
    let mut seen = [false; 27];
    for cell in &root.cells {
        let slot = usize::from(cell.loc.slot);
        if cell.loc.inv != pumpkin_cluster::inventory::INV_MAIN
            || slot >= 27
            || seen[slot]
            || *cell != cells[slot]
        {
            return false;
        }
        seen[slot] = true;
    }
    cells.iter().enumerate().all(|(slot, cell)| {
        cell.loc
            == pumpkin_cluster::inventory::InvLoc::new(
                pumpkin_cluster::inventory::INV_MAIN,
                slot as u16,
            )
            && (seen[slot] || cell.is_empty())
    })
}

#[cfg(test)]
mod canonical_qualification_tests {
    use super::*;
    use pumpkin_cluster::inventory::{
        INV_MAIN, InvLoc, InventoryCell, InventoryStateSnapshot, InventoryTarget,
    };
    #[test]
    fn source_newer_or_undone_ender_image_waits_for_matching_receiver_truth() {
        let mut cells =
            std::array::from_fn(|slot| InventoryCell::empty(InvLoc::new(INV_MAIN, slot as u16)));
        let player = pumpkin_cluster::identity::GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(2),
            pumpkin_cluster::identity::PlayerSlot(7),
        );
        let target = InventoryTarget::LeasedVirtual {
            host: player.server,
            key: uuid::Uuid::from_u128(42),
            player,
            incarnation: 9,
            session: 0,
            kind: 0x100,
        };
        let mut root = InventoryStateSnapshot {
            target,
            revision: 15,
            cells: cells.to_vec(),
        };
        assert!(matches_root_cells(&root, &cells));
        root.cells.pop();
        assert!(!matches_root_cells(&root, &cells));
        root.cells = cells.to_vec();
        cells[26] = super::super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, 26),
            &pumpkin_data::item_stack::ItemStack::new(17, &pumpkin_data::item::Item::DIAMOND),
        )
        .unwrap();
        assert!(!matches_root_cells(&root, &cells));
        root.cells = cells.to_vec();
        assert!(matches_root_cells(&root, &cells));
        cells[26].count = 3;
        assert!(!matches_root_cells(&root, &cells));
        root.revision = 2;
        root.cells = cells.to_vec();
        assert!(matches_root_cells(&root, &cells));
        root.cells[25] = root.cells[26].clone();
        assert!(!matches_root_cells(&root, &cells));
    }
}
