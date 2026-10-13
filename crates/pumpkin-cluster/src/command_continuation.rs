use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::canonical_action::ActionRef;
use crate::issuer_entity_admission::CommandPermissionSnapshot;
use crate::player_command_control::{CommandPrincipal, PlayerCommandRequest};
use crate::realm_completion::PeerGeneration;

pub const MAX_CONTINUATION_SLOTS: u64 = 12_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandContinuationProof {
    pub origin: PeerGeneration,
    pub principal: CommandPrincipal,
    pub permission: CommandPermissionSnapshot,
    pub source_session: u64,
    pub original_correlation: [u8; 16],
    pub original_request_digest: [u8; 32],
    pub predecessor: ActionRef,
    pub original_slot: u64,
    pub expires_slot: u64,
    pub remaining_ast_digest: [u8; 32],
    pub issued_slot: u64,
    pub issued_command_digest: [u8; 32],
    pub binding_digest: [u8; 32],
}

pub fn command_digest(request: &PlayerCommandRequest) -> Result<[u8; 32], postcard::Error> {
    let bytes = postcard::to_allocvec(&(
        request.source,
        request.source_incarnation,
        &request.principal,
        request.permission_snapshot,
        &request.target,
        request.correlation,
        &request.mutation,
    ))?;
    Ok(Sha256::digest(bytes).into())
}

impl CommandContinuationProof {
    fn digest(&self) -> Result<[u8; 32], postcard::Error> {
        let bytes = postcard::to_allocvec(&(
            self.origin,
            &self.principal,
            self.permission,
            self.source_session,
            self.original_correlation,
            self.original_request_digest,
            self.predecessor,
            self.original_slot,
            self.expires_slot,
            self.remaining_ast_digest,
            self.issued_slot,
            self.issued_command_digest,
        ))?;
        Ok(Sha256::digest(bytes).into())
    }

    pub fn seal(&mut self, request: &PlayerCommandRequest) -> Result<(), postcard::Error> {
        self.issued_command_digest = command_digest(request)?;
        self.binding_digest = self.digest()?;
        Ok(())
    }

    pub fn valid(&self) -> bool {
        self.origin.generation != 0
            && self.original_correlation != [0; 16]
            && self.original_request_digest != [0; 32]
            && self.remaining_ast_digest != [0; 32]
            && self.permission.valid_for(&self.principal)
            && self.original_slot as u16 == self.predecessor.tick.0
            && self.original_slot <= self.issued_slot
            && self.issued_slot <= self.expires_slot
            && self
                .expires_slot
                .checked_sub(self.original_slot)
                .is_some_and(|duration| duration <= MAX_CONTINUATION_SLOTS)
            && match &self.principal {
                CommandPrincipal::Console => self.source_session == 0,
                CommandPrincipal::Player { gid, uuid, .. } => {
                    gid.server == self.origin.server && *uuid != [0; 16] && self.source_session != 0
                }
            }
            && self
                .digest()
                .is_ok_and(|digest| digest == self.binding_digest)
    }

    pub fn matches(&self, request: &PlayerCommandRequest) -> bool {
        self.valid()
            && request.source == self.origin.server
            && request.source_incarnation == self.origin.generation
            && request.principal == self.principal
            && request.permission_snapshot == Some(self.permission)
            && command_digest(request).is_ok_and(|digest| digest == self.issued_command_digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::ActionId;
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::player_command_control::{CommandTarget, PlayerCommandMutation};
    use crate::time::TickStamp;

    fn fixture() -> (PlayerCommandRequest, CommandContinuationProof) {
        let mut request = PlayerCommandRequest {
            source: ServerId(2),
            source_incarnation: 7,
            principal: CommandPrincipal::Console,
            permission_snapshot: Some(CommandPermissionSnapshot {
                revision: 0,
                level: 4,
            }),
            continuation: None,
            target: CommandTarget::Player {
                gid: GlobalPlayerId::new(ServerId(4), PlayerSlot(1)),
                uuid: [1; 16],
            },
            correlation: [9; 16],
            mutation: PlayerCommandMutation::Tags {
                tag: "accepted-chain".into(),
                present: true,
            },
        };
        let mut proof = CommandContinuationProof {
            origin: PeerGeneration {
                server: ServerId(2),
                generation: 7,
            },
            principal: request.principal.clone(),
            permission: request.permission_snapshot.unwrap(),
            source_session: 0,
            original_correlation: [2; 16],
            original_request_digest: [3; 32],
            predecessor: ActionRef {
                id: ActionId {
                    actor: ActorId::Peer(ServerId(2)),
                    incarnation: 7,
                    sequence: 1,
                    family: 1,
                },
                tick: TickStamp(40),
            },
            original_slot: 65536 + 40,
            expires_slot: 65536 + 40 + MAX_CONTINUATION_SLOTS,
            remaining_ast_digest: [4; 32],
            issued_slot: 65536 + 140,
            issued_command_digest: [0; 32],
            binding_digest: [0; 32],
        };
        proof.seal(&request).unwrap();
        request.continuation = Some(Box::new(proof.clone()));
        assert!(request.is_valid());
        (request, proof)
    }

    #[test]
    fn issued_command_cannot_change_target_mutation_or_correlation() {
        let (request, proof) = fixture();
        let mut changed = request.clone();
        changed.target = CommandTarget::Player {
            gid: GlobalPlayerId::new(ServerId(4), PlayerSlot(2)),
            uuid: [2; 16],
        };
        assert!(!proof.matches(&changed));
        let mut changed = request.clone();
        changed.mutation = PlayerCommandMutation::Kill;
        assert!(!proof.matches(&changed));
        let mut changed = request;
        changed.correlation = [8; 16];
        assert!(!proof.matches(&changed));
    }

    #[test]
    fn original_ast_predecessor_principal_and_source_are_bound() {
        let (request, proof) = fixture();
        let mut changed = proof.clone();
        changed.remaining_ast_digest[0] ^= 1;
        assert!(!changed.matches(&request));
        let mut changed = proof.clone();
        changed.predecessor.id.sequence += 1;
        assert!(!changed.matches(&request));
        let mut changed = proof.clone();
        changed.origin.generation += 1;
        assert!(!changed.matches(&request));
        let mut changed = proof;
        changed.principal = CommandPrincipal::Player {
            gid: GlobalPlayerId::new(ServerId(2), PlayerSlot(3)),
            uuid: [5; 16],
            permission_revision: 6,
        };
        assert!(!changed.matches(&request));
    }

    #[test]
    fn expiry_rejects_new_issuance_without_reexpiring_accepted_proof() {
        let (request, mut proof) = fixture();
        let accepted = proof.clone();
        proof.issued_slot = proof.expires_slot + 1;
        proof.seal(&request).unwrap();
        assert!(!proof.matches(&request));
        assert!(accepted.matches(&request));
        let mut changed = accepted;
        changed.expires_slot = changed.original_slot + MAX_CONTINUATION_SLOTS + 1;
        changed.seal(&request).unwrap();
        assert!(!changed.matches(&request));
    }
}
