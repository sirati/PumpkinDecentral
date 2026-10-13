use crate::canonical_action::ActionRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MenuPresentationIdentity {
    pub session: u64,
    pub sync_id: u8,
}

impl MenuPresentationIdentity {
    pub fn from_source(reference: ActionRef, full_source_slot: u64) -> Option<Self> {
        if full_source_slot >= 1 << 47 || full_source_slot as u16 != reference.tick.0 || reference.id.incarnation == 0 {
            return None;
        }
        let session = (1 << 63) | (full_source_slot << 16) | u64::from(reference.id.sequence);
        (session != u64::MAX).then_some(Self { session, sync_id: 1 + (reference.id.sequence % 100) as u8 })
    }
}
