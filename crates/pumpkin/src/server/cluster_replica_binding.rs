use pumpkin_cluster::canonical_action::LivePlayerLease;
use pumpkin_cluster::realm_completion::PeerGeneration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaBindingNotice {
    pub lease: LivePlayerLease,
    pub source: PeerGeneration,
    pub client_entity_id: i32,
    pub retired: bool,
}

impl ReplicaBindingNotice {
    pub fn valid(self) -> bool {
        self.lease.incarnation != 0
            && !self.lease.key.0.is_nil()
            && self.source.server == self.lease.gid.server
            && self.source.generation != 0
            && self.client_entity_id > 0
    }
}

pub(crate) fn submit(server: &super::Server, notice: ReplicaBindingNotice) -> bool {
    notice.valid()
        && server
            .cluster_replica_binding_route
            .load_full()
            .is_some_and(|route| route.try_send(notice).is_ok())
}
