use crate::canonical_action::{LivePlayerLease, PlayerKey};
use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use crate::inventory::{INV_MAIN, INV_MAX_STACK, INVENTORY_MAX_NBT_BYTES, InvLoc, InventoryCell};
use crate::realm_completion::PeerGeneration;

const MAGIC: &[u8; 4] = b"PRE1";
const HEADER_BYTES: usize = 65;
const MAX_BYTES: usize = HEADER_BYTES + 27 * 7 + INVENTORY_MAX_NBT_BYTES;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlayerEnderSnapshot {
    pub lease: LivePlayerLease,
    pub source: PeerGeneration,
    pub install_generation: u64,
    pub sequence: u64,
    pub revision: Option<u64>,
    pub cells: Option<Box<[InventoryCell; 27]>>,
}

impl PlayerEnderSnapshot {
    pub fn is_wire(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }

    pub fn valid(&self) -> bool {
        self.lease.incarnation != 0
            && !self.lease.key.0.is_nil()
            && self.source.server == self.lease.gid.server
            && self.source.generation != 0
            && self.install_generation != 0
            && self.revision.is_some() == self.cells.is_some()
            && self.cells.as_ref().is_none_or(|cells| {
                let mut total = 0_usize;
                cells.iter().enumerate().all(|(slot, cell)| {
                    total = match total.checked_add(cell.nbt.len()) {
                        Some(total) => total,
                        None => return false,
                    };
                    cell.loc == InvLoc::new(INV_MAIN, slot as u16)
                        && total <= INVENTORY_MAX_NBT_BYTES
                        && valid_stack(cell.item, cell.count, cell.nbt.len())
                })
            })
    }

    pub fn is_newer_than(&self, previous: &Self) -> bool {
        self.source != previous.source
            || self.sequence != previous.sequence
                && self.sequence.wrapping_sub(previous.sequence) < (1_u64 << 63)
    }

    pub fn matches(
        &self,
        lease: LivePlayerLease,
        source: PeerGeneration,
        install_generation: u64,
    ) -> bool {
        self.lease == lease
            && self.source == source
            && self.install_generation == install_generation
    }

    pub fn encode(&self) -> Option<Vec<u8>> {
        if !self.valid() {
            return None;
        }
        let payload = self
            .cells
            .as_ref()
            .map_or(0, |cells| cells.iter().map(|cell| 7 + cell.nbt.len()).sum());
        let mut bytes = Vec::with_capacity(HEADER_BYTES + payload);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.lease.gid.server.0.to_be_bytes());
        bytes.extend_from_slice(&self.lease.gid.player.0.to_be_bytes());
        bytes.extend_from_slice(self.lease.key.0.as_bytes());
        bytes.extend_from_slice(&self.lease.incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.source.generation.to_be_bytes());
        bytes.extend_from_slice(&self.install_generation.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.push(u8::from(self.cells.is_some()));
        bytes.extend_from_slice(&self.revision.unwrap_or(0).to_be_bytes());
        if let Some(cells) = &self.cells {
            for cell in cells.iter() {
                bytes.extend_from_slice(&cell.item.to_be_bytes());
                bytes.push(cell.count);
                bytes.extend_from_slice(&(cell.nbt.len() as u32).to_be_bytes());
                bytes.extend_from_slice(&cell.nbt);
            }
        }
        Some(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_BYTES || bytes.len() > MAX_BYTES || !Self::is_wire(bytes) {
            return None;
        }
        let mut offset = 4;
        let gid = GlobalPlayerId::new(
            ServerId(u16::from_be_bytes(take(bytes, &mut offset)?)),
            PlayerSlot(u16::from_be_bytes(take(bytes, &mut offset)?)),
        );
        let uuid = uuid::Uuid::from_bytes(take(bytes, &mut offset)?);
        let incarnation = u64::from_be_bytes(take(bytes, &mut offset)?);
        let generation = u64::from_be_bytes(take(bytes, &mut offset)?);
        let install_generation = u64::from_be_bytes(take(bytes, &mut offset)?);
        let sequence = u64::from_be_bytes(take(bytes, &mut offset)?);
        let [available] = take(bytes, &mut offset)?;
        let revision = u64::from_be_bytes(take(bytes, &mut offset)?);
        let mut result = Self {
            lease: LivePlayerLease {
                key: PlayerKey(uuid),
                gid,
                incarnation,
            },
            source: PeerGeneration {
                server: gid.server,
                generation,
            },
            install_generation,
            sequence,
            revision: None,
            cells: None,
        };
        if !result.valid() {
            return None;
        }
        if available == 0 {
            return (bytes.len() == HEADER_BYTES && revision == 0).then_some(result);
        }
        if available != 1 {
            return None;
        }
        let start = offset;
        let mut total = 0_usize;
        for _ in 0..27 {
            let item = u16::from_be_bytes(take(bytes, &mut offset)?);
            let [count] = take(bytes, &mut offset)?;
            let length = usize::try_from(u32::from_be_bytes(take(bytes, &mut offset)?)).ok()?;
            total = total.checked_add(length)?;
            if total > INVENTORY_MAX_NBT_BYTES || !valid_stack(item, count, length) {
                return None;
            }
            let end = offset.checked_add(length)?;
            bytes.get(offset..end)?;
            offset = end;
        }
        if offset != bytes.len() {
            return None;
        }
        offset = start;
        let mut cells = Vec::with_capacity(27);
        for slot in 0..27 {
            let item = u16::from_be_bytes(take(bytes, &mut offset)?);
            let [count] = take(bytes, &mut offset)?;
            let length = usize::try_from(u32::from_be_bytes(take(bytes, &mut offset)?)).ok()?;
            let end = offset.checked_add(length)?;
            let nbt = bytes.get(offset..end)?.to_vec();
            offset = end;
            cells.push(InventoryCell {
                loc: InvLoc::new(INV_MAIN, slot),
                item,
                count,
                nbt,
            });
        }
        result.revision = Some(revision);
        result.cells = Some(Box::new(cells.try_into().ok()?));
        Some(result)
    }
}

fn valid_stack(item: u16, count: u8, nbt_length: usize) -> bool {
    if item == 0 || count == 0 {
        item == 0 && count == 0 && nbt_length == 0
    } else {
        count <= INV_MAX_STACK && nbt_length <= INVENTORY_MAX_NBT_BYTES
    }
}
fn take<const N: usize>(bytes: &[u8], offset: &mut usize) -> Option<[u8; N]> {
    let end = offset.checked_add(N)?;
    let value = bytes.get(*offset..end)?.try_into().ok()?;
    *offset = end;
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> PlayerEnderSnapshot {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let mut cells =
            std::array::from_fn(|slot| InventoryCell::empty(InvLoc::new(INV_MAIN, slot as u16)));
        cells[26] = InventoryCell {
            loc: InvLoc::new(INV_MAIN, 26),
            item: 3,
            count: 5,
            nbt: vec![1, 2, 3],
        };
        PlayerEnderSnapshot {
            lease: LivePlayerLease {
                key: PlayerKey(uuid::Uuid::from_u128(42)),
                gid,
                incarnation: 9,
            },
            source: PeerGeneration {
                server: gid.server,
                generation: 12,
            },
            install_generation: 80,
            sequence: 3,
            revision: Some(15),
            cells: Some(Box::new(cells)),
        }
    }
    #[test]
    fn complete_and_unavailable_roots_roundtrip_without_fabricating_empty_inventory() {
        let original = snapshot();
        assert_eq!(
            PlayerEnderSnapshot::decode(&original.encode().unwrap()),
            Some(original.clone())
        );
        let missing = PlayerEnderSnapshot {
            revision: None,
            cells: None,
            ..original
        };
        assert_eq!(
            PlayerEnderSnapshot::decode(&missing.encode().unwrap()),
            Some(missing)
        );
    }
    #[test]
    fn undo_revision_and_successor_session_use_publication_order_and_exact_binding() {
        let original = snapshot();
        let undo = PlayerEnderSnapshot {
            revision: Some(2),
            sequence: 4,
            ..original.clone()
        };
        assert!(undo.is_newer_than(&original));
        assert!(!original.is_newer_than(&undo));
        assert!(!undo.matches(
            original.lease,
            original.source,
            original.install_generation + 1
        ));
        let mut next = original.lease;
        next.incarnation += 1;
        assert!(!undo.matches(next, original.source, original.install_generation));
        let older = PlayerEnderSnapshot {
            sequence: u64::MAX,
            ..original.clone()
        };
        let wrapped = PlayerEnderSnapshot {
            sequence: 0,
            ..original
        };
        assert!(wrapped.is_newer_than(&older));
    }
    #[test]
    fn oversized_or_truncated_cell_lengths_are_rejected_before_cell_allocation() {
        let mut bytes = snapshot().encode().unwrap();
        bytes[HEADER_BYTES + 3..HEADER_BYTES + 7].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(PlayerEnderSnapshot::decode(&bytes).is_none());
        let mut bytes = snapshot().encode().unwrap();
        bytes.pop();
        assert!(PlayerEnderSnapshot::decode(&bytes).is_none());
        let mut bytes = snapshot().encode().unwrap();
        bytes.push(0);
        assert!(PlayerEnderSnapshot::decode(&bytes).is_none());
        let mut maximum = snapshot();
        maximum.cells.as_mut().unwrap()[26].nbt = vec![7; INVENTORY_MAX_NBT_BYTES];
        assert_eq!(
            PlayerEnderSnapshot::decode(&maximum.encode().unwrap()),
            Some(maximum)
        );
    }
}
