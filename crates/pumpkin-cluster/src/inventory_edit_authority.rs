use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{Action, ClientOrigin, LivePlayerLease, SemanticIntent};
use crate::canonical_inventory::InventorySlotBinding;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryPermissionWitness {
    pub issuer: PeerGeneration,
    pub viewer: LivePlayerLease,
    pub tick: TickStamp,
    pub policy_revision: u64,
    pub decision_version: u64,
    pub node: String,
    pub allowed: bool,
}

impl InventoryPermissionWitness {
    fn valid_for(&self, authority: &InventoryEditAuthority, node: &str) -> bool {
        self.allowed && self.issuer == authority.issuer && self.viewer == authority.viewer
            && self.tick == authority.tick && self.node == node
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryEditAuthority {
    pub issuer: PeerGeneration,
    pub viewer: LivePlayerLease,
    pub target: LivePlayerLease,
    pub tick: TickStamp,
    pub policy: InventoryEditPolicy,
    pub screen_session: u64,
    pub screen_install_generation: u64,
    pub preview_provider: Option<PeerGeneration>,
    pub bindings: Vec<InventorySlotBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryEditPolicy {
    OwnedScreen,
    Privileged(InventoryPermissionWitness),
    PrivilegedView(InventoryPermissionWitness),
}

impl InventoryEditAuthority {
    pub fn valid_for(&self, action: &Action) -> bool {
        if self.issuer.generation == 0 || self.issuer.server != self.viewer.gid.server
            || action.id.actor != ActorId::Player(self.viewer.gid)
            || action.id.incarnation != self.viewer.incarnation || action.tick != self.tick
            || !action.player_leases.contains(&self.viewer) || self.screen_install_generation == 0
            || self.preview_provider.is_some_and(|provider| provider.generation == 0
                || provider.server != self.target.gid.server)
            || self.bindings.len() > 128 || self.bindings.iter().any(|binding| !binding.is_valid())
            || self.bindings.iter().map(|binding| binding.target).collect::<std::collections::BTreeSet<_>>().len() != self.bindings.len()
        { return false; }
        match &self.policy {
            InventoryEditPolicy::PrivilegedView(permission) => {
                permission.valid_for(self, "minecraft:command.invsee") && self.target != self.viewer
                    && self.preview_provider.is_some() && self.bindings.is_empty()
                    && action.client_origin == ClientOrigin::None
                    && matches!(&action.semantic_intent, Some(SemanticIntent::MenuSessionChange(intent))
                        if intent.lease == self.viewer && matches!(&intent.operation,
                            crate::canonical_menu::MenuSessionOperation::Open { state, .. }
                                if state.session == self.screen_session && matches!(state.source,
                                    crate::canonical_menu::MenuSource::RemotePlayer { lease } if lease == self.target)))
            }
            policy => {
                let authorized = match policy {
                    InventoryEditPolicy::OwnedScreen => self.target == self.viewer,
                    InventoryEditPolicy::Privileged(permission) => self.target != self.viewer
                        && action.player_leases.contains(&self.target)
                        && permission.valid_for(self, "minecraft:command.invsee.edit"),
                    InventoryEditPolicy::PrivilegedView(_) => false,
                };
                authorized && !self.bindings.is_empty()
                    && matches!(action.client_origin,
                        ClientOrigin::JavaClick { session, .. } | ClientOrigin::BedrockStackRequest { session, .. }
                            if session == self.screen_session)
                    && matches!(&action.semantic_intent, Some(SemanticIntent::InventoryGestures(intent))
                        if intent.bindings.iter().all(|binding| self.allows(binding)))
            }
        }
    }

    pub fn allows(&self, binding: &InventorySlotBinding) -> bool {
        self.bindings.iter().any(|permitted| permitted == binding)
    }
}
