use std::collections::HashMap;
use std::hash::Hash;

use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};

pub const MAX_PAGE_BYTES: usize = 256 * 1024;
pub const MAX_LOGICAL_BYTES: usize = 64 * 1024 * 1024 + 64 * 1024;
const MAGIC: &[u8; 8] = b"PCAP0001";
const HEADER_BYTES: usize = 48;
const PAYLOAD_BYTES: usize = MAX_PAGE_BYTES - HEADER_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageError {
    EmptyLogical,
    LogicalTooLarge,
    InvalidHeader,
    PageTooLarge,
    InvalidCount,
    InvalidIndex,
    InvalidPayloadLength,
    InconsistentMessage,
    ConflictingDuplicate,
    DigestMismatch,
    MessageCapacity,
    ByteCapacity,
}

impl std::fmt::Display for PageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PageError {}

pub struct EncodedPages {
    logical: Bytes,
    digest: [u8; 32],
    count: u16,
    next: u16,
}

pub fn encode(logical: Bytes) -> Result<EncodedPages, PageError> {
    validate_total(logical.len())?;
    let count = u16::try_from(logical.len().div_ceil(PAYLOAD_BYTES))
        .map_err(|_| PageError::InvalidCount)?;
    Ok(EncodedPages {
        digest: Sha256::digest(&logical).into(),
        logical,
        count,
        next: 0,
    })
}

impl EncodedPages {
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn page_count(&self) -> u16 {
        self.count
    }
}

impl Iterator for EncodedPages {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.count {
            return None;
        }
        let offset = usize::from(self.next) * PAYLOAD_BYTES;
        let end = self.logical.len().min(offset + PAYLOAD_BYTES);
        let mut wire = BytesMut::with_capacity(HEADER_BYTES + end - offset);
        wire.extend_from_slice(MAGIC);
        wire.extend_from_slice(&self.digest);
        wire.extend_from_slice(&(self.logical.len() as u32).to_be_bytes());
        wire.extend_from_slice(&self.count.to_be_bytes());
        wire.extend_from_slice(&self.next.to_be_bytes());
        wire.extend_from_slice(&self.logical[offset..end]);
        self.next += 1;
        Some(wire.freeze())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.count - self.next);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for EncodedPages {}

pub struct ActionPage {
    digest: [u8; 32],
    total: usize,
    count: u16,
    index: u16,
    payload: Bytes,
}

impl ActionPage {
    pub fn logical_len(&self) -> usize {
        self.total
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

pub fn is_page(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

pub fn decode(wire: Bytes) -> Result<ActionPage, PageError> {
    if wire.len() > MAX_PAGE_BYTES {
        return Err(PageError::PageTooLarge);
    }
    if wire.len() < HEADER_BYTES || &wire[..8] != MAGIC {
        return Err(PageError::InvalidHeader);
    }
    let digest = wire[8..40]
        .try_into()
        .map_err(|_| PageError::InvalidHeader)?;
    let total = u32::from_be_bytes(
        wire[40..44]
            .try_into()
            .map_err(|_| PageError::InvalidHeader)?,
    ) as usize;
    validate_total(total)?;
    let count = u16::from_be_bytes(
        wire[44..46]
            .try_into()
            .map_err(|_| PageError::InvalidHeader)?,
    );
    let index = u16::from_be_bytes(
        wire[46..48]
            .try_into()
            .map_err(|_| PageError::InvalidHeader)?,
    );
    if usize::from(count) != total.div_ceil(PAYLOAD_BYTES) {
        return Err(PageError::InvalidCount);
    }
    if index >= count {
        return Err(PageError::InvalidIndex);
    }
    let expected = PAYLOAD_BYTES.min(total - usize::from(index) * PAYLOAD_BYTES);
    if wire.len() - HEADER_BYTES != expected {
        return Err(PageError::InvalidPayloadLength);
    }
    Ok(ActionPage {
        digest,
        total,
        count,
        index,
        payload: wire.slice(HEADER_BYTES..),
    })
}

fn validate_total(total: usize) -> Result<(), PageError> {
    if total == 0 {
        Err(PageError::EmptyLogical)
    } else if total > MAX_LOGICAL_BYTES {
        Err(PageError::LogicalTooLarge)
    } else {
        Ok(())
    }
}

struct Pending {
    total: usize,
    count: u16,
    reservation: usize,
    received: usize,
    parts: Vec<Option<Bytes>>,
}

#[derive(Clone, Copy)]
struct Completed {
    total: usize,
    count: u16,
}

pub struct Reassembler<K> {
    pending: HashMap<(K, [u8; 32]), Pending>,
    completed: HashMap<(K, [u8; 32]), Completed>,
    max_messages: usize,
    max_retained_bytes: usize,
    retained_bytes: usize,
}

impl<K: Clone + Eq + Hash> Reassembler<K> {
    pub fn new(max_messages: usize, max_retained_bytes: usize) -> Self {
        Self {
            pending: HashMap::new(),
            completed: HashMap::new(),
            max_messages,
            max_retained_bytes,
            retained_bytes: 0,
        }
    }

    pub fn push(&mut self, key: K, page: ActionPage) -> Result<Option<Bytes>, PageError> {
        let identity = (key, page.digest);
        if let Some(completed) = self.completed.get(&identity) {
            if completed.total != page.total || completed.count != page.count {
                return Err(PageError::InconsistentMessage);
            }
            return Ok(None);
        }
        if !self.pending.contains_key(&identity) {
            if self.pending.len() + self.completed.len() >= self.max_messages {
                return Err(PageError::MessageCapacity);
            }
            let reservation = page.total * 2 + usize::from(page.count) * HEADER_BYTES;
            if reservation > self.max_retained_bytes.saturating_sub(self.retained_bytes) {
                return Err(PageError::ByteCapacity);
            }
            self.retained_bytes += reservation;
            self.pending.insert(
                identity.clone(),
                Pending {
                    total: page.total,
                    count: page.count,
                    reservation,
                    received: 0,
                    parts: vec![None; usize::from(page.count)],
                },
            );
        }
        let Some(pending) = self.pending.get_mut(&identity) else {
            return Err(PageError::InconsistentMessage);
        };
        if pending.total != page.total || pending.count != page.count {
            return Err(PageError::InconsistentMessage);
        }
        let part = &mut pending.parts[usize::from(page.index)];
        if let Some(prior) = part {
            return if *prior == page.payload {
                Ok(None)
            } else {
                Err(PageError::ConflictingDuplicate)
            };
        }
        *part = Some(Bytes::copy_from_slice(&page.payload));
        pending.received += 1;
        if pending.received != usize::from(pending.count) {
            return Ok(None);
        }
        let Some(pending) = self.pending.remove(&identity) else {
            return Err(PageError::InconsistentMessage);
        };
        let mut logical = BytesMut::with_capacity(pending.total);
        for part in pending.parts {
            let Some(part) = part else {
                self.retained_bytes -= pending.reservation;
                return Err(PageError::InconsistentMessage);
            };
            logical.extend_from_slice(&part);
        }
        self.retained_bytes -= pending.reservation;
        if <[u8; 32]>::from(Sha256::digest(&logical)) != identity.1 {
            return Err(PageError::DigestMismatch);
        }
        self.completed.insert(
            identity,
            Completed {
                total: pending.total,
                count: pending.count,
            },
        );
        Ok(Some(logical.freeze()))
    }

    pub fn retain_keys(&mut self, mut keep: impl FnMut(&K) -> bool) {
        self.pending.retain(|(key, _), pending| {
            if keep(key) {
                true
            } else {
                self.retained_bytes -= pending.reservation;
                false
            }
        });
        self.completed.retain(|(key, _), _| keep(key));
    }

    pub fn clear_key(&mut self, key: &K) {
        self.retain_keys(|candidate| candidate != key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logical(seed: u8, len: usize) -> Bytes {
        (0..len)
            .map(|index| seed.wrapping_add((index % 251) as u8))
            .collect::<Vec<_>>()
            .into()
    }

    #[test]
    fn reordered_duplicate_pages_complete_once_without_partial_payload() {
        let body = logical(3, PAYLOAD_BYTES * 2 + 17);
        let pages = encode(body.clone()).unwrap().collect::<Vec<_>>();
        let mut receiver = Reassembler::new(4, body.len() * 3);
        assert!(
            receiver
                .push((7, 9), decode(pages[2].clone()).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(
            receiver
                .push((7, 9), decode(pages[0].clone()).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(
            receiver
                .push((7, 9), decode(pages[2].clone()).unwrap())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            receiver
                .push((7, 9), decode(pages[1].clone()).unwrap())
                .unwrap()
                .unwrap(),
            body
        );
        for page in pages {
            assert!(
                receiver
                    .push((7, 9), decode(page).unwrap())
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn generation_isolation_and_lifecycle_pruning_preserve_byte_and_message_limits() {
        let body = logical(7, PAYLOAD_BYTES + 3);
        let pages = encode(body.clone()).unwrap().collect::<Vec<_>>();
        let budget = body.len() * 2 + HEADER_BYTES * 2;
        let mut receiver = Reassembler::new(1, budget);
        assert!(
            receiver
                .push((1, 2), decode(pages[0].clone()).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            receiver.push((1, 3), decode(pages[1].clone()).unwrap()),
            Err(PageError::MessageCapacity)
        ));
        assert_eq!(
            receiver
                .push((1, 2), decode(pages[1].clone()).unwrap())
                .unwrap()
                .unwrap(),
            body
        );
        assert!(matches!(
            receiver.push((1, 3), decode(pages[0].clone()).unwrap()),
            Err(PageError::MessageCapacity)
        ));
        receiver.clear_key(&(1, 2));
        assert!(
            receiver
                .push((1, 3), decode(pages[1].clone()).unwrap())
                .unwrap()
                .is_none()
        );
        receiver.clear_key(&(1, 3));
        let mut receiver = Reassembler::new(3, budget);
        receiver.push(1, decode(pages[0].clone()).unwrap()).unwrap();
        assert!(matches!(
            receiver.push(2, decode(pages[1].clone()).unwrap()),
            Err(PageError::ByteCapacity)
        ));
        receiver.clear_key(&1);
        assert!(
            receiver
                .push(2, decode(pages[1].clone()).unwrap())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            receiver
                .push(2, decode(pages[0].clone()).unwrap())
                .unwrap()
                .unwrap(),
            body
        );
    }

    #[test]
    fn corrupted_pages_never_publish_and_failed_assembly_releases_capacity() {
        let body = logical(13, PAYLOAD_BYTES + 11);
        let pages = encode(body.clone()).unwrap().collect::<Vec<_>>();
        let mut corrupted = pages[0].to_vec();
        corrupted[HEADER_BYTES + 3] ^= 0x40;
        let mut receiver = Reassembler::new(1, body.len() * 3);
        receiver.push(1, decode(corrupted.into()).unwrap()).unwrap();
        assert!(matches!(
            receiver.push(1, decode(pages[0].clone()).unwrap()),
            Err(PageError::ConflictingDuplicate)
        ));
        assert!(matches!(
            receiver.push(1, decode(pages[1].clone()).unwrap()),
            Err(PageError::DigestMismatch)
        ));
        receiver.push(1, decode(pages[1].clone()).unwrap()).unwrap();
        assert_eq!(
            receiver
                .push(1, decode(pages[0].clone()).unwrap())
                .unwrap()
                .unwrap(),
            body
        );
    }

    #[test]
    fn conflicting_total_cannot_replace_an_existing_message() {
        let body = logical(19, PAYLOAD_BYTES + 11);
        let pages = encode(body.clone()).unwrap().collect::<Vec<_>>();
        let mut changed = pages[0].to_vec();
        changed[40..44].copy_from_slice(&(body.len() as u32 + 1).to_be_bytes());
        let mut receiver = Reassembler::new(1, body.len() * 3);
        receiver.push(2, decode(pages[0].clone()).unwrap()).unwrap();
        assert!(matches!(
            receiver.push(2, decode(changed.into()).unwrap()),
            Err(PageError::InconsistentMessage)
        ));
        assert_eq!(
            receiver
                .push(2, decode(pages[1].clone()).unwrap())
                .unwrap()
                .unwrap(),
            body
        );
    }

    #[test]
    fn wire_metadata_and_truncation_are_rejected_before_retention() {
        let wire = encode(logical(5, 17)).unwrap().next().unwrap();
        for cut in 0..wire.len() {
            assert!(decode(wire.slice(..cut)).is_err());
        }
        for (range, replacement) in [
            (
                40..44,
                (MAX_LOGICAL_BYTES as u32 + 1).to_be_bytes().to_vec(),
            ),
            (44..46, 2u16.to_be_bytes().to_vec()),
            (46..48, 1u16.to_be_bytes().to_vec()),
        ] {
            let mut changed = wire.to_vec();
            changed[range].copy_from_slice(&replacement);
            assert!(decode(changed.into()).is_err());
        }
        assert!(matches!(encode(Bytes::new()), Err(PageError::EmptyLogical)));
    }

    #[test]
    fn maximum_logical_batch_round_trips_in_bounded_wire_pages() {
        let body = logical(29, MAX_LOGICAL_BYTES);
        let mut receiver = Reassembler::new(1, MAX_LOGICAL_BYTES * 3);
        let mut complete = None;
        for page in encode(body.clone()).unwrap() {
            assert!(page.len() <= MAX_PAGE_BYTES);
            let result = receiver.push(3, decode(page).unwrap()).unwrap();
            if result.is_some() {
                assert!(complete.is_none());
                complete = result;
            }
        }
        assert_eq!(complete.unwrap(), body);
        let oversized = Bytes::from(vec![0; MAX_LOGICAL_BYTES + 1]);
        assert!(matches!(encode(oversized), Err(PageError::LogicalTooLarge)));
    }
}
