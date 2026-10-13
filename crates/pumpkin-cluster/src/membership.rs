//! Cluster membership with unanimous join votes and chunk directory sync.
//!
//! Join flow is always hello, then votes, then admit:
//! candidate broadcasts [`JoinHello`], every member replies with [`JoinVote`],
//! and admission broadcasts [`AdmitBroadcast`] only after every member approved.
//! Admitted peers then publish chunk ownership with directory adverts and
//! withdraw it with drops; losing a peer drops every chunk it held.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::chunks::{ChunkAdvert, ChunkDrop, Directory};
use crate::protocol::{ChunkKey, StreamKind};

pub const CLUSTER_PROTOCOL_VERSION: u16 = 68;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterNodeRole {
    Primary,
    Secondary,
}

/// Live members of one cluster.
#[derive(Debug, Default)]
pub struct Membership {
    /// Admitted peer ids.
    pub members: HashSet<u16>,
}

impl Membership {
    /// Seeds a membership from admitted peer ids.
    #[must_use]
    pub fn new(members: &[u16]) -> Self {
        let mut all = HashSet::new();
        all.extend(members.iter().copied());
        Self { members: all }
    }

    /// Reports whether a peer is admitted.
    #[must_use]
    pub fn contains(&self, peer: u16) -> bool {
        self.members.contains(&peer)
    }

    /// Counts admitted peers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Reports whether no peer is admitted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Admits a peer that completed the join vote.
    pub fn admit(&mut self, peer: u16) {
        self.members.insert(peer);
    }

    /// Removes a peer without touching votes or chunk holds.
    pub fn remove(&mut self, peer: u16) {
        self.members.remove(&peer);
    }

    /// Lists members in deterministic broadcast order.
    #[must_use]
    pub fn sorted_members(&self) -> Vec<u16> {
        let mut ordered: Vec<u16> = self.members.iter().copied().collect();
        ordered.sort_unstable();
        ordered
    }

    /// Lists members except one peer, in broadcast order.
    #[must_use]
    pub fn others(&self, exclude: u16) -> Vec<u16> {
        let mut ordered: Vec<u16> = self
            .members
            .iter()
            .copied()
            .filter(|peer| *peer != exclude)
            .collect();
        ordered.sort_unstable();
        ordered
    }
}

/// Approval votes keyed by join candidate.
#[derive(Debug, Default)]
pub struct JoinVotes {
    /// Voters that approved each candidate.
    pub votes: HashMap<u16, HashSet<u16>>,
}

impl JoinVotes {
    /// Starts empty vote storage.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one approval and reports unanimity.
    ///
    /// Returns true only when every member in `membership` approved `candidate`.
    pub fn record(&mut self, membership: &Membership, candidate: u16, voter: u16) -> bool {
        let voters = self.votes.entry(candidate).or_default();
        voters.insert(voter);
        self.is_unanimous(membership, candidate)
    }

    /// Lists voters that approved a candidate.
    #[must_use]
    pub fn voters_for(&self, candidate: u16) -> Vec<u16> {
        match self.votes.get(&candidate) {
            Some(voters) => {
                let mut ordered: Vec<u16> = voters.iter().copied().collect();
                ordered.sort_unstable();
                ordered
            }
            None => Vec::new(),
        }
    }

    /// Reports unanimity for a candidate against current members.
    #[must_use]
    pub fn is_unanimous(&self, membership: &Membership, candidate: u16) -> bool {
        match self.votes.get(&candidate) {
            Some(voters) => membership
                .members
                .iter()
                .all(|member| voters.contains(member)),
            None => false,
        }
    }

    /// Discards all votes for one candidate, usually after admit or deny.
    pub fn discard_candidate(&mut self, candidate: u16) {
        self.votes.remove(&candidate);
    }

    /// Discards one voter from every candidate, usually after eviction.
    pub fn discard_voter(&mut self, voter: u16) {
        for voters in self.votes.values_mut() {
            voters.remove(&voter);
        }
    }
}

/// Reports whether a peer drained players and entities and may leave.
#[must_use]
pub const fn can_leave_network(player_count: usize, owned_entities: usize) -> bool {
    player_count == 0 && owned_entities == 0
}

/// Reports the stream that carries [`JoinHandshake`] messages.
#[must_use]
pub const fn handshake_channel() -> StreamKind {
    StreamKind::Control
}

/// First join step: candidate announces itself with its certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinHello {
    /// Joining peer id.
    pub candidate: u16,
    /// Certificate fingerprint receivers pin against.
    pub cert_fingerprint: [u8; 32],
    pub incarnation: u64,
    pub protocol_version: u16,
    pub role: ClusterNodeRole,
}

/// Second join step: a member asks every other member to vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinVoteRequest {
    /// Joining peer id.
    pub candidate: u16,
    /// Member asking for votes.
    pub requested_by: u16,
}

/// Third join step: one member approves or denies one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinVote {
    /// Joining peer id.
    pub candidate: u16,
    /// Voting member.
    pub voter: u16,
    /// False denies the join and blocks admission.
    pub approve: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerView {
    pub reporter: u16,
    pub connected: Vec<u16>,
}

/// Final join step: admission announced after a full approval vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmitBroadcast {
    /// Admitted peer id.
    pub candidate: u16,
    /// Member announcing admission.
    pub admitted_by: u16,
}

/// Wire envelope for the hello, votes, and admit flow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinHandshake {
    /// Candidate announcement.
    Hello(JoinHello),
    /// Member fan-out asking for votes.
    VoteRequest(JoinVoteRequest),
    /// Single approval or denial.
    Vote(JoinVote),
    /// Unanimous admission announcement.
    Admit(AdmitBroadcast),
    PeerView(PeerView),
}

pub const ADMISSION_SETTLE_MILLIS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct DynamicAdmission {
    candidate: u16,
    started_millis: u64,
    direct: HashSet<u16>,
    views: HashMap<u16, HashSet<u16>>,
    acknowledged: HashSet<u16>,
}

impl DynamicAdmission {
    #[must_use]
    pub fn new(candidate: u16, started_millis: u64) -> Self {
        Self {
            candidate,
            started_millis,
            direct: HashSet::new(),
            views: HashMap::new(),
            acknowledged: HashSet::new(),
        }
    }

    pub fn connected(&mut self, peer: u16) {
        if peer != self.candidate {
            self.direct.insert(peer);
        }
    }

    pub fn disconnected(&mut self, peer: u16) {
        self.direct.remove(&peer);
        self.views.remove(&peer);
        self.acknowledged.remove(&peer);
        for view in self.views.values_mut() {
            view.remove(&peer);
        }
    }

    pub fn advertise(&mut self, view: PeerView) -> bool {
        if !self.direct.contains(&view.reporter) {
            return false;
        }
        self.views.insert(
            view.reporter,
            view.connected
                .into_iter()
                .filter(|peer| *peer != self.candidate)
                .collect(),
        );
        true
    }

    pub fn acknowledge(&mut self, peer: u16) {
        if peer != self.candidate {
            self.acknowledged.insert(peer);
        }
    }

    #[must_use]
    pub fn required(&self) -> HashSet<u16> {
        let mut required = self.direct.clone();
        for reporter in &self.direct {
            if let Some(view) = self.views.get(reporter) {
                required.extend(view.iter().copied());
            }
        }
        required.remove(&self.candidate);
        required
    }

    #[must_use]
    pub fn direct_peers(&self) -> Vec<u16> {
        let mut peers: Vec<u16> = self.direct.iter().copied().collect();
        peers.sort_unstable();
        peers
    }

    #[must_use]
    pub fn admitted(&self, now_millis: u64) -> bool {
        if self.direct.is_empty() {
            return false;
        }
        if !self.direct.iter().all(|peer| self.views.contains_key(peer)) {
            return false;
        }
        let required = self.required();
        required.is_subset(&self.direct)
            || (now_millis.saturating_sub(self.started_millis) >= ADMISSION_SETTLE_MILLIS
                && required.is_subset(&self.acknowledged))
    }
}

/// Codec failure for [`JoinHandshake`] envelopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeCodecError {
    /// Human readable failure.
    pub message: String,
}

impl core::fmt::Display for HandshakeCodecError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for HandshakeCodecError {}

/// Encodes a join handshake envelope.
pub fn encode_handshake(message: &JoinHandshake) -> Result<Vec<u8>, HandshakeCodecError> {
    postcard::to_allocvec(message).map_err(|error| HandshakeCodecError {
        message: format!("encode handshake: {error}"),
    })
}

/// Decodes a join handshake envelope.
pub fn decode_handshake(bytes: &[u8]) -> Result<JoinHandshake, HandshakeCodecError> {
    postcard::from_bytes(bytes).map_err(|error| HandshakeCodecError {
        message: format!("decode handshake: {error}"),
    })
}

/// Outcome of applying one [`JoinVote`] to a [`ClusterState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteOutcome {
    /// Still waiting for more approvals.
    Waiting,
    /// Vote completed unanimity and admitted the candidate.
    Admitted(AdmitBroadcast),
    /// Vote denied the candidate; admission is blocked.
    Denied {
        /// Denied candidate.
        candidate: u16,
        /// Denying voter.
        voter: u16,
    },
}

/// Membership plus votes plus chunk directory for one peer.
///
/// `ClusterState` documents the full order: hello announces a candidate,
/// votes collect one approval per member, admit inserts the member, adverts
/// publish that member's chunks, and drops or eviction withdraw them.
#[derive(Debug, Default)]
pub struct ClusterState {
    local: u16,
    membership: Membership,
    votes: JoinVotes,
    directory: Directory,
}

impl ClusterState {
    /// Seeds a cluster view that already contains the local peer.
    #[must_use]
    pub fn new(local: u16, members: &[u16]) -> Self {
        let mut seeded: Vec<u16> = members.to_vec();
        if !seeded.contains(&local) {
            seeded.push(local);
        }
        Self {
            local,
            membership: Membership::new(&seeded),
            votes: JoinVotes::new(),
            directory: Directory::new(),
        }
    }

    /// Reports the local peer id.
    #[must_use]
    pub const fn local(&self) -> u16 {
        self.local
    }

    /// Borrows current membership.
    #[must_use]
    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Borrows the chunk holder directory.
    #[must_use]
    pub const fn directory(&self) -> &Directory {
        &self.directory
    }

    /// Counts admitted members.
    #[must_use]
    pub fn member_count(&self) -> usize {
        self.membership.len()
    }

    /// Counts chunks with at least one holder.
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.directory.len()
    }

    /// Reports whether a peer is admitted.
    #[must_use]
    pub fn is_member(&self, peer: u16) -> bool {
        self.membership.contains(peer)
    }

    /// Reports whether a candidate still needs the hello path.
    #[must_use]
    pub fn is_new_candidate(&self, candidate: u16) -> bool {
        candidate != self.local && !self.membership.contains(candidate)
    }

    /// Builds this peer's hello announcement.
    #[must_use]
    pub const fn hello(
        &self,
        cert_fingerprint: [u8; 32],
        incarnation: u64,
        role: ClusterNodeRole,
    ) -> JoinHello {
        JoinHello {
            candidate: self.local,
            cert_fingerprint,
            incarnation,
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            role,
        }
    }

    /// Builds a vote request for a candidate.
    #[must_use]
    pub const fn vote_request(&self, candidate: u16) -> JoinVoteRequest {
        JoinVoteRequest {
            candidate,
            requested_by: self.local,
        }
    }

    /// Builds a local approval for a candidate.
    #[must_use]
    pub const fn approval(&self, candidate: u16) -> JoinVote {
        JoinVote {
            candidate,
            voter: self.local,
            approve: true,
        }
    }

    /// Builds a local denial for a candidate.
    #[must_use]
    pub const fn denial(&self, candidate: u16) -> JoinVote {
        JoinVote {
            candidate,
            voter: self.local,
            approve: false,
        }
    }

    /// Applies one vote and admits on unanimity.
    ///
    /// Denials return [`VoteOutcome::Denied`] without recording. Approvals
    /// record and return [`VoteOutcome::Admitted`] exactly when the last
    /// missing member approved; otherwise they return [`VoteOutcome::Waiting`].
    pub fn record_vote(&mut self, vote: JoinVote) -> VoteOutcome {
        let candidate = vote.candidate;
        if self.membership.contains(candidate) {
            return VoteOutcome::Waiting;
        }
        if !vote.approve {
            self.votes.discard_candidate(candidate);
            return VoteOutcome::Denied {
                candidate,
                voter: vote.voter,
            };
        }
        if self.votes.record(&self.membership, candidate, vote.voter) {
            self.admit_locked(candidate, vote.voter)
        } else {
            VoteOutcome::Waiting
        }
    }

    /// Records one approval and admits on unanimity.
    pub fn record_approval(&mut self, candidate: u16, voter: u16) -> Option<AdmitBroadcast> {
        match self.record_vote(JoinVote {
            candidate,
            voter,
            approve: true,
        }) {
            VoteOutcome::Admitted(admit) => Some(admit),
            VoteOutcome::Waiting | VoteOutcome::Denied { .. } => None,
        }
    }

    /// Applies a remote admit as one approval vote from its sender.
    ///
    /// Returns true only when this approval newly admitted the candidate.
    pub fn apply_admit(&mut self, admit: AdmitBroadcast) -> bool {
        if self.membership.contains(admit.candidate) {
            return false;
        }
        self.votes
            .record(&self.membership, admit.candidate, admit.admitted_by);
        if self.votes.is_unanimous(&self.membership, admit.candidate) {
            self.membership.admit(admit.candidate);
            self.votes.discard_candidate(admit.candidate);
            true
        } else {
            false
        }
    }

    /// Publishes one chunk hold for an admitted member.
    ///
    /// Returns false and keeps the directory unchanged when the holder is
    /// not admitted.
    pub fn apply_advert(&mut self, advert: ChunkAdvert) -> bool {
        if !self.membership.contains(advert.holder) {
            return false;
        }
        self.directory.apply_advert(advert);
        true
    }

    /// Withdraws one chunk hold previously published by advert.
    pub fn apply_drop(&mut self, drop: ChunkDrop) -> bool {
        let before = self.directory.sorted_holders(&drop.chunk);
        self.directory.apply_drop(drop);
        before != self.directory.sorted_holders(&drop.chunk)
    }

    /// Removes a peer and withdraws every chunk it held.
    ///
    /// Eviction clears the peer from membership, drops it as a voter and as
    /// a candidate, prunes it from the directory, and returns each touched
    /// chunk so callers can refetch.
    pub fn evict(&mut self, peer: u16) -> Vec<ChunkKey> {
        self.membership.remove(peer);
        self.votes.discard_candidate(peer);
        self.votes.discard_voter(peer);
        self.directory.drop_holder(peer)
    }

    /// Lists chunk holders in deterministic order.
    #[must_use]
    pub fn sorted_holders(&self, chunk: &ChunkKey) -> Vec<u16> {
        self.directory.sorted_holders(chunk)
    }

    fn admit_locked(&mut self, candidate: u16, admitted_by: u16) -> VoteOutcome {
        self.membership.admit(candidate);
        self.votes.discard_candidate(candidate);
        VoteOutcome::Admitted(AdmitBroadcast {
            candidate,
            admitted_by,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::WorldId;

    const TEST_WORLD: WorldId = WorldId::new(0, 1);

    #[test]
    fn join_needs_unanimity() {
        let members = Membership::new(&[1, 2, 3]);
        let mut votes = JoinVotes::new();
        assert!(!votes.record(&members, 4, 1));
        assert!(!votes.record(&members, 4, 2));
        assert!(votes.record(&members, 4, 3));
    }

    #[test]
    fn leave_requires_empty_peer() {
        assert!(can_leave_network(0, 0));
        assert!(!can_leave_network(1, 0));
        assert!(!can_leave_network(0, 1));
    }

    #[test]
    fn handshake_travels_on_control() {
        assert_eq!(handshake_channel(), StreamKind::Control);
    }

    #[test]
    fn dynamic_admission_uses_live_peer_views_instead_of_bootstrap_configuration() {
        let mut admission = DynamicAdmission::new(9, 1_000);
        admission.connected(1);
        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: vec![2, 3],
        }));
        admission.acknowledge(1);
        admission.acknowledge(2);
        assert!(!admission.admitted(11_000));
        admission.acknowledge(3);
        assert!(admission.admitted(11_000));
    }

    #[test]
    fn direct_full_mesh_admits_without_the_settle_delay() {
        let mut admission = DynamicAdmission::new(9, 10_000);
        admission.connected(1);
        admission.connected(2);
        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: vec![2],
        }));
        assert!(!admission.admitted(10_000));
        assert!(admission.advertise(PeerView {
            reporter: 2,
            connected: vec![1],
        }));
        assert!(admission.admitted(10_000));
    }

    #[test]
    fn disconnected_peer_is_removed_from_admission_requirements() {
        let mut admission = DynamicAdmission::new(9, 0);
        admission.connected(1);
        admission.connected(2);
        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: vec![2],
        }));
        admission.disconnected(2);
        assert!(admission.admitted(0));
        assert_eq!(admission.required(), HashSet::from([1]));
    }

    #[test]
    fn disconnected_candidate_does_not_self_admit_after_settle_window() {
        let mut admission = DynamicAdmission::new(9, 0);
        admission.connected(1);
        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: Vec::new(),
        }));
        admission.acknowledge(1);
        admission.disconnected(1);

        assert!(!admission.admitted(10_000));
        assert!(admission.required().is_empty());
    }

    #[test]
    fn advertised_set_expansion_keeps_original_settle_deadline() {
        let mut admission = DynamicAdmission::new(9, 1_000);
        admission.connected(1);
        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: vec![2],
        }));
        admission.acknowledge(1);
        admission.acknowledge(2);
        assert!(!admission.admitted(10_999));

        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: vec![2, 3],
        }));
        admission.acknowledge(3);
        assert!(admission.admitted(11_000));
    }

    #[test]
    fn every_direct_peer_must_publish_a_view_before_admission() {
        let mut admission = DynamicAdmission::new(9, 0);
        admission.connected(1);
        admission.connected(2);
        assert!(admission.advertise(PeerView {
            reporter: 1,
            connected: vec![2],
        }));
        admission.acknowledge(1);
        admission.acknowledge(2);

        assert!(!admission.admitted(10_000));
        assert!(admission.advertise(PeerView {
            reporter: 2,
            connected: vec![1],
        }));
        assert!(admission.admitted(10_000));
    }

    #[test]
    fn handshake_roundtrips() {
        let messages = [
            JoinHandshake::Hello(JoinHello {
                candidate: 4,
                cert_fingerprint: [9_u8; 32],
                incarnation: 7,
                protocol_version: CLUSTER_PROTOCOL_VERSION,
                role: ClusterNodeRole::Secondary,
            }),
            JoinHandshake::VoteRequest(JoinVoteRequest {
                candidate: 4,
                requested_by: 1,
            }),
            JoinHandshake::Vote(JoinVote {
                candidate: 4,
                voter: 2,
                approve: true,
            }),
            JoinHandshake::Admit(AdmitBroadcast {
                candidate: 4,
                admitted_by: 1,
            }),
        ];
        for message in messages {
            let bytes = encode_handshake(&message).unwrap();
            assert_eq!(decode_handshake(&bytes).unwrap(), message);
        }
    }

    #[test]
    fn handshake_rejects_garbage() {
        assert!(decode_handshake(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn hello_votes_admit_then_adverts() {
        let mut cluster = ClusterState::new(1, &[1, 2]);
        let candidate = 3_u16;
        assert!(cluster.is_new_candidate(candidate));
        let hello = JoinHello {
            candidate,
            cert_fingerprint: [7_u8; 32],
            incarnation: 7,
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            role: ClusterNodeRole::Secondary,
        };
        assert_eq!(hello.candidate, candidate);
        assert!(cluster.record_approval(candidate, 1).is_none());
        let admit = cluster.record_approval(candidate, 2).unwrap();
        assert_eq!(admit.candidate, candidate);
        assert!(cluster.is_member(candidate));
        assert!(!cluster.is_new_candidate(candidate));

        let chunk = ChunkKey::new(TEST_WORLD, 0, 0);
        assert!(cluster.apply_advert(ChunkAdvert {
            holder: candidate,
            chunk
        }));
        assert_eq!(cluster.sorted_holders(&chunk), vec![candidate]);
        assert_eq!(cluster.chunk_count(), 1);
    }

    #[test]
    fn denial_blocks_admission() {
        let mut cluster = ClusterState::new(1, &[1, 2]);
        let outcome = cluster.record_vote(JoinVote {
            candidate: 3,
            voter: 2,
            approve: false,
        });
        assert_eq!(
            outcome,
            VoteOutcome::Denied {
                candidate: 3,
                voter: 2
            }
        );
        assert!(!cluster.is_member(3));
    }

    #[test]
    fn remote_admit_counts_as_single_vote() {
        let mut cluster = ClusterState::new(1, &[1, 2]);
        assert!(!cluster.apply_admit(AdmitBroadcast {
            candidate: 3,
            admitted_by: 1,
        }));
        assert!(!cluster.is_member(3));
        assert!(cluster.apply_admit(AdmitBroadcast {
            candidate: 3,
            admitted_by: 2,
        }));
        assert!(cluster.is_member(3));
    }

    #[test]
    fn drops_withdraw_adverts_and_evict_clears_holder() {
        let mut cluster = ClusterState::new(1, &[1, 2]);
        let first = ChunkKey::new(TEST_WORLD, 1, 1);
        let second = ChunkKey::new(TEST_WORLD, 2, 2);
        assert!(cluster.apply_advert(ChunkAdvert {
            holder: 2,
            chunk: first
        }));
        assert!(cluster.apply_advert(ChunkAdvert {
            holder: 2,
            chunk: second
        }));
        assert!(cluster.apply_drop(ChunkDrop {
            holder: 2,
            chunk: first
        }));
        assert!(cluster.sorted_holders(&first).is_empty());
        assert_eq!(cluster.sorted_holders(&second), vec![2]);

        let touched = cluster.evict(2);
        assert_eq!(touched, vec![second]);
        assert!(!cluster.is_member(2));
        assert!(cluster.directory().is_empty());
    }

    #[test]
    fn advert_from_non_member_is_rejected() {
        let mut cluster = ClusterState::new(1, &[1]);
        assert!(!cluster.apply_advert(ChunkAdvert {
            holder: 9,
            chunk: ChunkKey::new(TEST_WORLD, 0, 0)
        }));
        assert!(cluster.directory().is_empty());
    }
}
