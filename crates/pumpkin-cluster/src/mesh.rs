//! Mesh trust: locally generated certs with out-of-band pinning.
//!
//! Each server generates its QUIC keypair locally. Public certs travel over a
//! third channel owned by whoever performs the setup; Pumpkin never ships
//! private keys and runs no shared CA. Handshakes fail closed against the pin
//! list below. QUIC endpoint wiring builds on these types in a later phase.

use serde::{Deserialize, Serialize};

use crate::identity::ServerId;

/// Maximum uni streams per QUIC connection. Sized for four streams per player
/// plus entity, chunk and control streams with wide headroom.
pub const MAX_UNI_STREAMS: u32 = 1_000_000;

/// Remote peer pinned at setup time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedPeer {
    /// Peer id, unique cluster-wide.
    pub server: ServerId,
    /// Dial address, e.g. `203.0.113.7:24577`.
    pub addr: String,
    /// SHA-256 of the peer's public cert in DER form.
    pub pubkey_sha256: [u8; 32],
}

/// Local mesh endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    /// This server's id.
    pub server: ServerId,
    /// Bind address for incoming QUIC connections.
    pub bind_addr: String,
    /// Path to the locally generated public cert in DER form.
    pub cert_path: String,
    /// Path to the locally generated private key in DER form.
    pub key_path: String,
    /// Pinned remotes from the out-of-band setup exchange.
    pub peers: Vec<PinnedPeer>,
}

/// SHA-256 fingerprint of a DER cert. Falls back to zeros only if hash output
/// lengths ever change, which would already break the pin comparison.
#[must_use]
pub fn cert_fingerprint_sha256(cert_der: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(cert_der);
    match <[u8; 32]>::try_from(digest.as_slice()) {
        Ok(bytes) => bytes,
        Err(_) => [0_u8; 32],
    }
}

#[must_use]
pub fn peer_by_id(config: &MeshConfig, server: ServerId) -> Option<&PinnedPeer> {
    config.peers.iter().find(|peer| peer.server == server)
}

#[must_use]
pub fn peer_by_cert<'a>(peers: &'a [PinnedPeer], cert_der: &[u8]) -> Option<&'a PinnedPeer> {
    peers
        .iter()
        .find(|peer| peer_authorized(cert_der, peer))
}

/// True when the presented cert matches the setup-time pin.
#[must_use]
pub fn peer_authorized(cert_der: &[u8], pinned: &PinnedPeer) -> bool {
    cert_fingerprint_sha256(cert_der) == pinned.pubkey_sha256
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pinned_for(cert: &[u8]) -> PinnedPeer {
        PinnedPeer {
            server: ServerId(2),
            addr: String::from("203.0.113.7:24577"),
            pubkey_sha256: cert_fingerprint_sha256(cert),
        }
    }

    #[test]
    fn fingerprint_is_stable() {
        assert_eq!(
            cert_fingerprint_sha256(b"cert"),
            cert_fingerprint_sha256(b"cert")
        );
    }

    #[test]
    fn authorizes_pinned_cert_only() {
        let pinned = pinned_for(b"cert-a");
        assert!(peer_authorized(b"cert-a", &pinned));
        assert!(!peer_authorized(b"cert-b", &pinned));
    }

    fn mesh_with(peer: PinnedPeer) -> MeshConfig {
        MeshConfig {
            server: ServerId(1),
            bind_addr: String::from("127.0.0.1:0"),
            cert_path: String::from("cert.der"),
            key_path: String::from("key.der"),
            peers: vec![peer],
        }
    }

    #[test]
    fn finds_peers_by_id_and_cert() {
        let peer = pinned_for(b"cert-a");
        let mesh = mesh_with(peer);
        assert_eq!(
            peer_by_id(&mesh, ServerId(2)),
            Some(&mesh.peers[0])
        );
        assert_eq!(peer_by_id(&mesh, ServerId(9)), None);
        assert_eq!(
            peer_by_cert(&mesh.peers, b"cert-a"),
            Some(&mesh.peers[0])
        );
        assert_eq!(peer_by_cert(&mesh.peers, b"cert-b"), None);
    }
}
