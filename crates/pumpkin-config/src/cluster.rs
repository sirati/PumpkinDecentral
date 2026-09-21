use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClusterRole {
    Primary,
    #[default]
    Secondary,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PinnedPeerConfig {
    pub server_id: u16,
    pub addr: String,
    pub pubkey_sha256_hex: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub role: ClusterRole,
    pub server_id: u16,
    pub bind_addr: String,
    pub cert_path: String,
    pub key_path: String,
    pub peers: Vec<PinnedPeerConfig>,
    pub ntp_servers: Vec<String>,
    pub max_offset_millis: i64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            role: ClusterRole::Secondary,
            server_id: 0,
            bind_addr: String::from("0.0.0.0:24577"),
            cert_path: String::from("cluster-cert.der"),
            key_path: String::from("cluster-key.der"),
            peers: Vec::new(),
            ntp_servers: Vec::new(),
            max_offset_millis: 25,
        }
    }
}

impl ClusterConfig {
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        if !self.enabled {
            return Vec::new();
        }
        if self.cert_path.is_empty() {
            tracing::warn!("cluster enabled but cert_path is empty");
        } else if !std::path::Path::new(&self.cert_path).exists() {
            tracing::warn!(
                "cluster cert file '{}' does not exist yet; generate it before meshing",
                self.cert_path
            );
        }
        if self.key_path.is_empty() {
            tracing::warn!("cluster enabled but key_path is empty");
        } else if !std::path::Path::new(&self.key_path).exists() {
            tracing::warn!(
                "cluster key file '{}' does not exist yet; generate it before meshing",
                self.key_path
            );
        }
        let mut errors = Vec::new();
        let primary_first_boot = self.role == ClusterRole::Primary && self.peers.is_empty();
        if self.peers.is_empty() && !primary_first_boot {
            errors.push(String::from(
                "cluster enabled with no pinned peers; add peers or boot the primary first",
            ));
        }
        for peer in &self.peers {
            if peer.server_id == self.server_id {
                errors.push(format!(
                    "pinned peer {} reuses this server's own id {}",
                    peer.server_id, self.server_id
                ));
            }
            if peer.addr.trim().is_empty() {
                errors.push(format!("pinned peer {} has an empty address", peer.server_id));
            }
            if peer.pubkey_sha256_hex.len() != 64
                || !peer.pubkey_sha256_hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                errors.push(format!(
                    "pinned peer {} pubkey must be 64 hex chars (SHA-256)",
                    peer.server_id
                ));
            }
        }
        errors
    }
}
