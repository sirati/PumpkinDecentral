use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::canonical_action::ApplyFailure;
use crate::membership::ClusterNodeRole;
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoundingAdmission {
    pub certificate_fingerprint: [u8; 32],
    pub role: ClusterNodeRole,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkGenesisCertificate {
    pub chunk: ChunkKey,
    pub lineage: [u8; 32],
    pub initial_image: [u8; 32],
    pub founding: BTreeMap<PeerGeneration, FoundingAdmission>,
    pub issuer: PeerGeneration,
    pub full_source_slot: u64,
}

pub fn elected_primary(
    chunk: ChunkKey,
    lineage: [u8; 32],
    founding: &BTreeMap<PeerGeneration, FoundingAdmission>,
) -> Result<PeerGeneration, ApplyFailure> {
    if founding.is_empty() || lineage == [0; 32] {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut servers = BTreeSet::new();
    for (source, admission) in founding {
        if source.generation == 0 || admission.protocol_version == 0
            || admission.certificate_fingerprint == [0; 32] || !servers.insert(source.server)
        {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let encoded = postcard::to_allocvec(&(chunk, lineage, founding)).map_err(|_| ApplyFailure::InvalidAction)?;
    let mut basis = Sha256::new();
    basis.update(b"pumpkin-chunk-genesis-election-v1");
    basis.update(encoded);
    let basis = basis.finalize();
    let mut winner = None;
    for (source, admission) in founding {
        if admission.role != ClusterNodeRole::Primary { continue; }
        let mut candidate = Sha256::new();
        candidate.update(basis);
        candidate.update(source.server.0.to_le_bytes());
        candidate.update(source.generation.to_le_bytes());
        let candidate = (<[u8; 32]>::from(candidate.finalize()), *source);
        if winner.as_ref().is_none_or(|previous| candidate > *previous) { winner = Some(candidate); }
    }
    winner.map(|(_, source)| source).ok_or(ApplyFailure::InvalidAction)
}

impl ChunkGenesisCertificate {
    pub fn validate_structural(&self) -> Result<(), ApplyFailure> {
        if self.full_source_slot == 0 || self.initial_image == [0; 32]
            || elected_primary(self.chunk, self.lineage, &self.founding)? != self.issuer
        {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }

    pub fn identity(&self) -> Result<[u8; 32], ApplyFailure> {
        self.validate_structural()?;
        let encoded = postcard::to_allocvec(self).map_err(|_| ApplyFailure::InvalidAction)?;
        let mut identity = Sha256::new();
        identity.update(b"pumpkin-chunk-genesis-certificate-v1");
        identity.update(encoded);
        Ok(identity.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ServerId;
    use crate::protocol::WorldId;

    #[test]
    fn founding_election_ignores_creation_phase_and_checks_original_role() {
        let one = PeerGeneration { server: ServerId(1), generation: 11 };
        let two = PeerGeneration { server: ServerId(2), generation: 22 };
        let founding = BTreeMap::from([
            (one, FoundingAdmission {certificate_fingerprint: [1;32],role:ClusterNodeRole::Primary,protocol_version:1}),
            (two, FoundingAdmission {certificate_fingerprint: [2;32],role:ClusterNodeRole::Primary,protocol_version:1}),
        ]);
        let chunk = ChunkKey::new(WorldId::new(0,7),-5,9);
        let issuer = elected_primary(chunk,[9;32],&founding).unwrap();
        let first = ChunkGenesisCertificate {chunk,lineage:[9;32],initial_image:[8;32],founding,issuer,full_source_slot:42};
        let mut later = first.clone();
        later.full_source_slot += 32768;
        assert!(first.validate_structural().is_ok());
        assert!(later.validate_structural().is_ok());
        assert_eq!(first.issuer,later.issuer);
        assert_ne!(first.identity().unwrap(),later.identity().unwrap());
        later.issuer = if issuer == one {two} else {one};
        assert!(later.validate_structural().is_err());
        later.issuer = issuer;
        later.founding.get_mut(&issuer).unwrap().role = ClusterNodeRole::Secondary;
        assert!(later.validate_structural().is_err());
        let encoded = postcard::to_allocvec(&first).unwrap();
        let restored: ChunkGenesisCertificate = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(restored.identity(),first.identity());
    }
}
