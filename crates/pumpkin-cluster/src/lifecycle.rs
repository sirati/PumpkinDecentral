use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::membership::{
    AdmitBroadcast, JoinHandshake, JoinHello, JoinVote, JoinVoteRequest, JoinVotes, Membership,
    can_leave_network, handshake_channel,
};
use crate::protocol::ChunkAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    Solo,
    Joining,
    Member,
    Leaving,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OwnedEntity {
    pub local_id: i32,
    pub chunk: ChunkAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handoff {
    pub chunk: ChunkAddr,
    pub entity_local_ids: Vec<i32>,
    pub targets: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityHandoffRequest {
    pub leaver: u16,
    pub handoffs: Vec<Handoff>,
}

#[must_use]
pub fn plan_handoff(
    owned: &[OwnedEntity],
    holders: &dyn Fn(ChunkAddr) -> Vec<u16>,
) -> Vec<Handoff> {
    let mut grouped: BTreeMap<ChunkAddr, Vec<i32>> = BTreeMap::new();
    for entity in owned {
        grouped.entry(entity.chunk).or_default().push(entity.local_id);
    }
    grouped
        .into_iter()
        .map(|(chunk, entity_local_ids)| {
            let targets = holders(chunk);
            Handoff {
                chunk,
                entity_local_ids,
                targets,
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleInput {
    BeginJoin {
        cert_fingerprint: [u8; 32],
        peers: Vec<u16>,
    },
    RemoteHello {
        hello: JoinHello,
        pin_match: bool,
    },
    NotePeerRestarted {
        peer: u16,
    },
    NotePeerConnected {
        peer: u16,
    },
    RemoteVoteRequest(JoinVoteRequest),
    RemoteVote(JoinVote),
    RemoteAdmit(AdmitBroadcast),
    RemoteHandoff(EntityHandoffRequest),
    RequestLeave {
        player_count: usize,
        owned: Vec<OwnedEntity>,
        holders: Vec<(ChunkAddr, Vec<u16>)>,
    },
    NoteUncleanDrop {
        peer: u16,
    },
    OperatorClearSuspect {
        peer: u16,
    },
    CompleteLeave,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEffect {
    SendHello {
        to: Vec<u16>,
        hello: JoinHello,
    },
    SendVoteRequest {
        to: Vec<u16>,
        request: JoinVoteRequest,
    },
    SendVote {
        to: Vec<u16>,
        vote: JoinVote,
    },
    BroadcastAdmit {
        to: Vec<u16>,
        admit: AdmitBroadcast,
    },
    EmitHandoff(EntityHandoffRequest),
    Joined {
        candidate: u16,
    },
    Left {
        peer: u16,
    },
    LeaveBlocked {
        player_count: usize,
        owned_entities: usize,
    },
    VoteDenied {
        candidate: u16,
        voter: u16,
    },
    VoteBlocked {
        candidate: u16,
    },
    SuspectMarked {
        peer: u16,
    },
    SuspectCleared {
        peer: u16,
    },
    StateChanged(LifecycleState),
}

impl LifecycleEffect {
    #[must_use]
    pub const fn handshake(&self) -> Option<JoinHandshake> {
        match self {
            Self::SendHello { hello, .. } => Some(JoinHandshake::Hello(*hello)),
            Self::SendVoteRequest { request, .. } => {
                Some(JoinHandshake::VoteRequest(*request))
            }
            Self::SendVote { vote, .. } => Some(JoinHandshake::Vote(*vote)),
            Self::BroadcastAdmit { admit, .. } => Some(JoinHandshake::Admit(*admit)),
            Self::EmitHandoff(_)
            | Self::Joined { .. }
            | Self::Left { .. }
            | Self::LeaveBlocked { .. }
            | Self::VoteDenied { .. }
            | Self::VoteBlocked { .. }
            | Self::SuspectMarked { .. }
            | Self::SuspectCleared { .. }
            | Self::StateChanged(_) => None,
        }
    }

    #[must_use]
    pub const fn channel(&self) -> crate::protocol::StreamKind {
        handshake_channel()
    }
}

#[derive(Debug)]
pub struct Lifecycle {
    local_id: u16,
    state: LifecycleState,
    membership: Membership,
    votes: JoinVotes,
    suspects: HashSet<u16>,
    hellos: HashMap<u16, JoinHello>,
    local_fingerprint: [u8; 32],
}

impl Lifecycle {
    #[must_use]
    pub fn new_solo(local_id: u16) -> Self {
        Self {
            local_id,
            state: LifecycleState::Solo,
            membership: Membership::new(&[local_id]),
            votes: JoinVotes::new(),
            suspects: HashSet::new(),
            hellos: HashMap::new(),
            local_fingerprint: [0_u8; 32],
        }
    }

    #[must_use]
    pub fn new_with_members(local_id: u16, members: &[u16]) -> Self {
        let mut seeded: Vec<u16> = members.to_vec();
        if !seeded.contains(&local_id) {
            seeded.push(local_id);
        }
        let state = if seeded.len() > 1 {
            LifecycleState::Member
        } else {
            LifecycleState::Solo
        };
        Self {
            local_id,
            state,
            membership: Membership::new(&seeded),
            votes: JoinVotes::new(),
            suspects: HashSet::new(),
            hellos: HashMap::new(),
            local_fingerprint: [0_u8; 32],
        }
    }

    #[must_use]
    pub const fn local_id(&self) -> u16 {
        self.local_id
    }

    #[must_use]
    pub const fn state(&self) -> LifecycleState {
        self.state
    }

    #[must_use]
    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    #[must_use]
    pub fn is_suspect(&self, peer: u16) -> bool {
        self.suspects.contains(&peer)
    }

    #[must_use]
    pub const fn player_join_allowed(&self) -> bool {
        matches!(
            self.state,
            LifecycleState::Member | LifecycleState::Joining
        )
    }

    pub fn apply(&mut self, input: LifecycleInput) -> Vec<LifecycleEffect> {
        match input {
            LifecycleInput::BeginJoin {
                cert_fingerprint,
                peers,
            } => self.begin_join(cert_fingerprint, peers),
            LifecycleInput::RemoteHello { hello, pin_match } => {
                self.remote_hello(hello, pin_match)
            }
            LifecycleInput::NotePeerRestarted { peer } => self.note_peer_restarted(peer),
            LifecycleInput::NotePeerConnected { peer } => self.note_peer_connected(peer),
            LifecycleInput::RemoteVoteRequest(request) => self.remote_vote_request(request),
            LifecycleInput::RemoteVote(vote) => self.remote_vote(vote),
            LifecycleInput::RemoteAdmit(admit) => self.remote_admit(admit),
            LifecycleInput::RemoteHandoff(request) => self.remote_handoff(request),
            LifecycleInput::RequestLeave {
                player_count,
                owned,
                holders,
            } => self.request_leave(player_count, &owned, &holders),
            LifecycleInput::NoteUncleanDrop { peer } => self.note_unclean_drop(peer),
            LifecycleInput::OperatorClearSuspect { peer } => self.operator_clear_suspect(peer),
            LifecycleInput::CompleteLeave => self.complete_leave(),
        }
    }

    fn is_admitted(&self, candidate: u16) -> bool {
        if candidate == self.local_id {
            self.state == LifecycleState::Member
        } else {
            self.membership.contains(candidate)
        }
    }

    fn sorted_others(&self, exclude: u16) -> Vec<u16> {
        let mut others: Vec<u16> = self
            .membership
            .members
            .iter()
            .copied()
            .filter(|peer| *peer != exclude)
            .collect();
        others.sort_unstable();
        others
    }

    fn vote_recipients(&self, candidate: u16) -> Vec<u16> {
        let mut to = self.sorted_others(self.local_id);
        if !to.contains(&candidate) {
            to.push(candidate);
        }
        to.sort_unstable();
        to
    }

    fn set_state(&mut self, state: LifecycleState, effects: &mut Vec<LifecycleEffect>) {
        if self.state != state {
            self.state = state;
            effects.push(LifecycleEffect::StateChanged(state));
        }
    }

    /// Admits `candidate` after callers proved a full vote.
    ///
    /// Never call directly for a remote `AdmitBroadcast`; go through
    /// [`Self::check_unanimity`] so every current member must have voted.
    fn admit_candidate(&mut self, candidate: u16, effects: &mut Vec<LifecycleEffect>) {
        if self.is_admitted(candidate) {
            return;
        }
        self.membership.admit(candidate);
        self.votes.votes.remove(&candidate);
        let admit = AdmitBroadcast {
            candidate,
            admitted_by: self.local_id,
        };
        let to = self.sorted_others(self.local_id);
        effects.push(LifecycleEffect::BroadcastAdmit { to, admit });
        effects.push(LifecycleEffect::Joined { candidate });
    }

    /// Admits only when every current member voted for `candidate`.
    ///
    /// A join is a full vote: one missing voter blocks admission.
    fn check_unanimity(&mut self, candidate: u16, effects: &mut Vec<LifecycleEffect>) {
        if self.suspects.contains(&candidate) {
            return;
        }
        let approved = self.votes.votes.get(&candidate).is_some_and(|voters| {
            self.membership
                .members
                .iter()
                .filter(|member| !self.suspects.contains(member))
                .all(|member| voters.contains(member))
        });
        if !approved {
            return;
        }
        self.admit_candidate(candidate, effects);
        if candidate == self.local_id {
            self.set_state(LifecycleState::Member, effects);
        }
    }

    fn begin_join(
        &mut self,
        cert_fingerprint: [u8; 32],
        peers: Vec<u16>,
    ) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        if self.state != LifecycleState::Solo {
            return effects;
        }
        self.local_fingerprint = cert_fingerprint;
        let mut to = peers;
        to.sort_unstable();
        to.dedup();
        to.retain(|peer| *peer != self.local_id);
        self.set_state(LifecycleState::Joining, &mut effects);
        let hello = JoinHello {
            candidate: self.local_id,
            cert_fingerprint,
        };
        effects.push(LifecycleEffect::SendHello { to, hello });
        self.votes
            .record(&self.membership, self.local_id, self.local_id);
        self.check_unanimity(self.local_id, &mut effects);
        effects
    }

    fn note_peer_restarted(&mut self, peer: u16) -> Vec<LifecycleEffect> {
        if peer == self.local_id || !self.membership.contains(peer) {
            return Vec::new();
        }
        self.membership.remove(peer);
        self.votes.discard_candidate(peer);
        self.votes.discard_voter(peer);
        self.suspects.remove(&peer);
        let mut effects = vec![LifecycleEffect::Left { peer }];
        if let Some(hello) = self.hellos.get(&peer).copied() {
            effects.extend(self.remote_hello(hello, true));
        }
        let pending: Vec<u16> = self.votes.votes.keys().copied().collect();
        for candidate in pending {
            self.check_unanimity(candidate, &mut effects);
        }
        effects
    }

    fn note_peer_connected(&mut self, peer: u16) -> Vec<LifecycleEffect> {
        if peer == self.local_id
            || (self.state != LifecycleState::Joining && self.state != LifecycleState::Member)
            || self.membership.contains(peer)
            || self.suspects.contains(&peer)
        {
            return Vec::new();
        }
        vec![LifecycleEffect::SendHello {
            to: vec![peer],
            hello: JoinHello {
                candidate: self.local_id,
                cert_fingerprint: self.local_fingerprint,
            },
        }]
    }

    fn remote_hello(&mut self, hello: JoinHello, pin_match: bool) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        let candidate = hello.candidate;
        self.hellos.insert(candidate, hello);
        if candidate == self.local_id || self.membership.contains(candidate) {
            return effects;
        }
        if self.suspects.contains(&candidate) {
            effects.push(LifecycleEffect::VoteBlocked { candidate });
            return effects;
        }
        if !pin_match {
            effects.push(LifecycleEffect::SendVote {
                to: vec![candidate],
                vote: JoinVote {
                    candidate,
                    voter: self.local_id,
                    approve: false,
                },
            });
            return effects;
        }
        if self
            .votes
            .votes
            .get(&candidate)
            .is_some_and(|voters| voters.contains(&self.local_id))
        {
            return effects;
        }
        self.votes
            .record(&self.membership, candidate, self.local_id);
        let request = JoinVoteRequest {
            candidate,
            requested_by: self.local_id,
        };
        effects.push(LifecycleEffect::SendVoteRequest {
            to: self.sorted_others(self.local_id),
            request,
        });
        effects.push(LifecycleEffect::SendVote {
            to: self.vote_recipients(candidate),
            vote: JoinVote {
                candidate,
                voter: self.local_id,
                approve: true,
            },
        });
        self.check_unanimity(candidate, &mut effects);
        effects
    }

    fn remote_vote_request(&mut self, request: JoinVoteRequest) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        let candidate = request.candidate;
        if candidate == self.local_id || self.membership.contains(candidate) {
            return effects;
        }
        if self.suspects.contains(&candidate) {
            effects.push(LifecycleEffect::VoteBlocked { candidate });
            return effects;
        }
        self.votes
            .record(&self.membership, candidate, self.local_id);
        effects.push(LifecycleEffect::SendVote {
            to: self.vote_recipients(candidate),
            vote: JoinVote {
                candidate,
                voter: self.local_id,
                approve: true,
            },
        });
        self.check_unanimity(candidate, &mut effects);
        effects
    }

    fn remote_vote(&mut self, vote: JoinVote) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        let candidate = vote.candidate;
        if self.is_admitted(candidate) {
            return effects;
        }
        if self.suspects.contains(&candidate) {
            effects.push(LifecycleEffect::VoteBlocked { candidate });
            return effects;
        }
        if !vote.approve {
            effects.push(LifecycleEffect::VoteDenied {
                candidate,
                voter: vote.voter,
            });
            return effects;
        }
        self.votes
            .record(&self.membership, candidate, vote.voter);
        self.check_unanimity(candidate, &mut effects);
        effects
    }

    /// Learns about a remote admission without trusting it.
    ///
    /// An `AdmitBroadcast` counts as a single approval vote from
    /// `admitted_by`; admission still waits for [`Self::check_unanimity`],
    /// so peers admit a join only by full vote.
    fn remote_admit(&mut self, admit: AdmitBroadcast) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        let candidate = admit.candidate;
        if self.suspects.contains(&candidate) {
            effects.push(LifecycleEffect::VoteBlocked { candidate });
            return effects;
        }
        if self.is_admitted(candidate) {
            return effects;
        }
        self.votes
            .record(&self.membership, candidate, admit.admitted_by);
        self.check_unanimity(candidate, &mut effects);
        effects
    }

    /// Removes a clean leaver announced by handoff.
    ///
    /// Clean removal happens only here: the leaver must first drain players
    /// and entities (see [`Self::request_leave`]) and announce the empty
    /// handoff peers apply through this path. Unclean drops use the suspect
    /// path instead and need an operator to clear.
    fn remote_handoff(&mut self, request: EntityHandoffRequest) -> Vec<LifecycleEffect> {
        let leaver = request.leaver;
        if leaver == self.local_id || !self.membership.contains(leaver) {
            return Vec::new();
        }
        self.membership.remove(leaver);
        self.votes.votes.remove(&leaver);
        let mut effects = vec![LifecycleEffect::Left { peer: leaver }];
        let pending: Vec<u16> = self.votes.votes.keys().copied().collect();
        for candidate in pending {
            self.check_unanimity(candidate, &mut effects);
        }
        effects
    }

    /// Starts a clean leave only when drained, always announcing a handoff.
    ///
    /// Leave needs no players, no owned entities, plus an `EmitHandoff` peers
    /// remove through [`Self::remote_handoff`]. Occupied servers stay
    /// `Member` with `LeaveBlocked`; drained-but-owned servers emit the
    /// per-chunk handoff plan and stay until the next drained request.
    fn request_leave(
        &mut self,
        player_count: usize,
        owned: &[OwnedEntity],
        holders: &[(ChunkAddr, Vec<u16>)],
    ) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        if self.state != LifecycleState::Member {
            return effects;
        }
        if can_leave_network(player_count, owned.len()) {
            effects.push(LifecycleEffect::EmitHandoff(EntityHandoffRequest {
                leaver: self.local_id,
                handoffs: Vec::new(),
            }));
            self.set_state(LifecycleState::Leaving, &mut effects);
            return effects;
        }
        if player_count == 0 {
            let lookup = |chunk: ChunkAddr| {
                holders
                    .iter()
                    .find(|entry| entry.0 == chunk)
                    .map_or_else(Vec::new, |entry| entry.1.clone())
            };
            let handoffs = plan_handoff(owned, &lookup);
            effects.push(LifecycleEffect::EmitHandoff(EntityHandoffRequest {
                leaver: self.local_id,
                handoffs,
            }));
        }
        effects.push(LifecycleEffect::LeaveBlocked {
            player_count,
            owned_entities: owned.len(),
        });
        effects
    }

    fn note_unclean_drop(&mut self, peer: u16) -> Vec<LifecycleEffect> {
        if peer == self.local_id || !self.membership.contains(peer) {
            return Vec::new();
        }
        self.membership.remove(peer);
        self.votes.votes.remove(&peer);
        self.suspects.insert(peer);
        let mut effects = vec![LifecycleEffect::SuspectMarked { peer }];
        let pending: Vec<u16> = self.votes.votes.keys().copied().collect();
        for candidate in pending {
            self.check_unanimity(candidate, &mut effects);
        }
        effects
    }

    fn operator_clear_suspect(&mut self, peer: u16) -> Vec<LifecycleEffect> {
        if self.suspects.remove(&peer) {
            vec![LifecycleEffect::SuspectCleared { peer }]
        } else {
            Vec::new()
        }
    }

    /// Finishes a leave that [`Self::request_leave`] already announced.
    ///
    /// Reachable only from `Leaving`, which that announce step entered, so
    /// local reset never skips the peer-visible empty handoff.
    fn complete_leave(&mut self) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        if self.state != LifecycleState::Leaving {
            return effects;
        }
        self.membership = Membership::new(&[self.local_id]);
        self.votes = JoinVotes::new();
        self.set_state(LifecycleState::Solo, &mut effects);
        effects.push(LifecycleEffect::Left {
            peer: self.local_id,
        });
        effects
    }
}

#[must_use]
pub const fn can_accept_player() -> bool {
    true
}

pub async fn run_lifecycle(
    mut lifecycle: Lifecycle,
    inputs: &mut mpsc::Receiver<LifecycleInput>,
    effects: &mpsc::Sender<LifecycleEffect>,
) {
    while let Some(input) = inputs.recv().await {
        let out = lifecycle.apply(input);
        for effect in out {
            if effects.send(effect).await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FINGERPRINT: [u8; 32] = [7_u8; 32];

    fn member_node(local: u16) -> Lifecycle {
        Lifecycle::new_with_members(local, &[1, 2, 3])
    }

    fn hello(candidate: u16) -> LifecycleInput {
        LifecycleInput::RemoteHello {
            hello: JoinHello {
                candidate,
                cert_fingerprint: FINGERPRINT,
            },
            pin_match: true,
        }
    }

    fn vote(candidate: u16, voter: u16) -> LifecycleInput {
        LifecycleInput::RemoteVote(JoinVote {
            candidate,
            voter,
            approve: true,
        })
    }

    fn joined(effects: &[LifecycleEffect], candidate: u16) -> bool {
        effects
            .iter()
            .any(|effect| matches!(effect, LifecycleEffect::Joined { candidate: c } if *c == candidate))
    }

    #[test]
    fn unanimous_admit() {
        let mut node = member_node(1);
        let effects = node.apply(hello(4));
        assert!(
            effects.iter().any(
                |effect| matches!(effect, LifecycleEffect::SendVote { vote, .. } if vote.approve && vote.candidate == 4)
            )
        );
        assert!(!node.membership().contains(4));
        let effects = node.apply(vote(4, 2));
        assert!(!joined(&effects, 4));
        assert!(!node.membership().contains(4));
        let effects = node.apply(vote(4, 3));
        assert!(joined(&effects, 4));
        assert!(node.membership().contains(4));
    }

    #[test]
    fn non_unanimous_blocked() {
        let mut node = member_node(1);
        node.apply(hello(4));
        let effects = node.apply(vote(4, 2));
        assert!(!joined(&effects, 4));
        assert!(!node.membership().contains(4));
        assert_eq!(node.state(), LifecycleState::Member);
    }

    #[test]
    fn duplicate_hello_emits_votes_once() {
        let mut node = member_node(1);
        let first = node.apply(hello(4));
        assert!(
            first.iter().any(
                |effect| matches!(effect, LifecycleEffect::SendVote { vote, .. } if vote.approve && vote.candidate == 4)
            )
        );
        assert!(node.apply(hello(4)).is_empty());
        assert!(!node.membership().contains(4));
        let effects = node.apply(vote(4, 2));
        assert!(!joined(&effects, 4));
        let effects = node.apply(vote(4, 3));
        assert!(joined(&effects, 4));
        assert!(node.membership().contains(4));
        assert!(node.apply(hello(4)).is_empty());
    }

    #[test]
    fn link_up_rehello_reaches_unadmitted_peer() {
        let mut node = Lifecycle::new_solo(1);
        assert!(
            node.apply(LifecycleInput::NotePeerConnected { peer: 0 })
                .is_empty()
        );
        node.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![0],
        });
        let effects = node.apply(LifecycleInput::NotePeerConnected { peer: 0 });
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            LifecycleEffect::SendHello { to, hello } => {
                assert_eq!(*to, vec![0]);
                assert_eq!(hello.candidate, 1);
                assert_eq!(hello.cert_fingerprint, FINGERPRINT);
            }
            _ => panic!("expected a hello to the linked peer"),
        }
        assert!(
            node.apply(LifecycleInput::NotePeerConnected { peer: 1 })
                .is_empty()
        );
        let mut member = member_node(1);
        assert!(member.apply(hello(2)).is_empty());
        assert!(
            member
                .apply(LifecycleInput::NotePeerConnected { peer: 2 })
                .is_empty()
        );
    }

    #[test]
    fn restarted_member_rejoins_through_fresh_admission() {
        let mut node = member_node(1);
        assert!(node.apply(hello(2)).is_empty());
        assert!(node.membership().contains(2));
        let effects = node.apply(LifecycleInput::NotePeerRestarted { peer: 2 });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::Left { peer: 2 }))
        );
        assert!(!node.membership().contains(2));
        assert!(!joined(&effects, 2));
        let effects = node.apply(vote(2, 3));
        assert!(joined(&effects, 2));
        assert!(node.membership().contains(2));
        assert!(node.apply(hello(2)).is_empty());
    }

    #[test]
    fn rejoined_candidate_needs_fresh_votes() {
        let mut node = member_node(1);
        node.apply(hello(4));
        node.apply(vote(4, 2));
        node.apply(vote(4, 3));
        assert!(node.membership().contains(4));
        let effects = node.apply(LifecycleInput::NotePeerRestarted { peer: 4 });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::Left { peer: 4 }))
        );
        assert!(!node.membership().contains(4));
        let effects = node.apply(vote(4, 2));
        assert!(!joined(&effects, 4));
        let effects = node.apply(vote(4, 3));
        assert!(joined(&effects, 4));
        assert!(node.membership().contains(4));
    }

    #[test]
    fn restart_of_unknown_peer_is_ignored() {
        let mut node = member_node(1);
        assert!(
            node.apply(LifecycleInput::NotePeerRestarted { peer: 9 })
                .is_empty()
        );
        assert!(!node.membership().contains(9));
    }

    #[test]
    fn explicit_refusal_surfaces_without_admit() {
        let mut node = member_node(1);
        node.apply(hello(4));
        let effects = node.apply(LifecycleInput::RemoteVote(JoinVote {
            candidate: 4,
            voter: 2,
            approve: false,
        }));
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::VoteDenied { candidate: 4, voter: 2 }))
        );
        assert!(!node.membership().contains(4));
    }

    #[test]
    fn leave_blocked_while_occupied() {
        let mut node = member_node(1);
        let effects = node.apply(LifecycleInput::RequestLeave {
            player_count: 2,
            owned: Vec::new(),
            holders: Vec::new(),
        });
        assert!(
            effects.iter().any(
                |effect| matches!(effect, LifecycleEffect::LeaveBlocked { player_count: 2, owned_entities: 0 })
            )
        );
        assert_eq!(node.state(), LifecycleState::Member);
    }

    #[test]
    fn leave_with_entities_emits_handoff_and_stays() {
        let mut node = member_node(1);
        let chunk_a = ChunkAddr { x: 0, z: 0 };
        let chunk_b = ChunkAddr { x: 1, z: 0 };
        let owned = vec![
            OwnedEntity { local_id: 11, chunk: chunk_a },
            OwnedEntity { local_id: 12, chunk: chunk_a },
            OwnedEntity { local_id: 13, chunk: chunk_b },
        ];
        let effects = node.apply(LifecycleInput::RequestLeave {
            player_count: 0,
            owned,
            holders: vec![(chunk_a, vec![2, 3]), (chunk_b, vec![3])],
        });
        let request = effects.iter().find_map(|effect| match effect {
            LifecycleEffect::EmitHandoff(request) => Some(request),
            _ => None,
        });
        assert!(request.is_some());
        let request = request.expect("leave with entities emits a handoff");
        assert_eq!(request.leaver, 1);
        assert_eq!(request.handoffs.len(), 2);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::LeaveBlocked { owned_entities: 3, .. }))
        );
        assert_eq!(node.state(), LifecycleState::Member);
    }

    #[test]
    fn clean_leave_and_complete() {
        let mut node = member_node(1);
        let effects = node.apply(LifecycleInput::RequestLeave {
            player_count: 0,
            owned: Vec::new(),
            holders: Vec::new(),
        });
        assert_eq!(node.state(), LifecycleState::Leaving);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::StateChanged(LifecycleState::Leaving)))
        );
        let effects = node.apply(LifecycleInput::CompleteLeave);
        assert_eq!(node.state(), LifecycleState::Solo);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::Left { peer: 1 }))
        );
        assert!(node.membership().contains(1));
        assert_eq!(node.membership().len(), 1);
    }

    #[test]
    fn suspect_requires_operator_readmit() {
        let mut node = member_node(1);
        let effects = node.apply(LifecycleInput::NoteUncleanDrop { peer: 2 });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::SuspectMarked { peer: 2 }))
        );
        assert!(!node.membership().contains(2));
        assert!(node.is_suspect(2));

        let effects = node.apply(hello(2));
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::VoteBlocked { candidate: 2 }))
        );
        let effects = node.apply(vote(2, 3));
        assert!(!joined(&effects, 2));
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::VoteBlocked { candidate: 2 }))
        );
        let effects = node.apply(LifecycleInput::RemoteAdmit(AdmitBroadcast {
            candidate: 2,
            admitted_by: 3,
        }));
        assert!(!joined(&effects, 2));
        assert!(!node.membership().contains(2));

        let effects = node.apply(LifecycleInput::OperatorClearSuspect { peer: 2 });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::SuspectCleared { peer: 2 }))
        );
        assert!(!node.is_suspect(2));
        assert!(!node.membership().contains(2));

        node.apply(hello(2));
        let effects = node.apply(vote(2, 3));
        assert!(joined(&effects, 2));
        assert!(node.membership().contains(2));
    }

    #[test]
    fn candidate_self_join_reaches_member() {
        let mut node = Lifecycle::new_solo(5);
        let effects = node.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![6],
        });
        assert_eq!(node.state(), LifecycleState::Member);
        assert!(joined(&effects, 5));
        assert!(
            effects.iter().any(
                |effect| matches!(effect, LifecycleEffect::SendHello { to, .. } if to == &vec![6_u16])
            )
        );
    }

    #[test]
    fn solo_join_without_peers_admits_immediately() {
        let mut node = Lifecycle::new_solo(5);
        let effects = node.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: Vec::new(),
        });
        assert_eq!(node.state(), LifecycleState::Member);
        assert!(joined(&effects, 5));
    }

    #[test]
    fn player_joins_allowed_while_joining() {
        assert!(can_accept_player());
        assert!(!Lifecycle::new_solo(1).player_join_allowed());
        assert!(Lifecycle::new_with_members(1, &[1, 2]).player_join_allowed());
        let mut joining = Lifecycle::new_solo(9);
        joining.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![1, 2],
        });
        assert!(joining.player_join_allowed());
        let mut leaving = member_node(1);
        leaving.apply(LifecycleInput::RequestLeave {
            player_count: 0,
            owned: Vec::new(),
            holders: Vec::new(),
        });
        assert!(!leaving.player_join_allowed());
    }

    #[test]
    fn suspect_mark_unblocks_pending_join() {
        let mut node = member_node(1);
        node.apply(hello(9));
        let effects = node.apply(vote(9, 2));
        assert!(!joined(&effects, 9));
        let effects = node.apply(LifecycleInput::NoteUncleanDrop { peer: 3 });
        assert!(joined(&effects, 9));
        assert!(node.membership().contains(9));
    }

    #[test]
    fn plan_handoff_groups_per_chunk() {
        let chunk_a = ChunkAddr { x: 0, z: 0 };
        let chunk_b = ChunkAddr { x: 0, z: 1 };
        let owned = vec![
            OwnedEntity { local_id: 1, chunk: chunk_b },
            OwnedEntity { local_id: 2, chunk: chunk_a },
            OwnedEntity { local_id: 3, chunk: chunk_a },
        ];
        let handoffs = plan_handoff(&owned, &|chunk| {
            if chunk == chunk_a {
                vec![2_u16, 3_u16]
            } else {
                Vec::new()
            }
        });
        assert_eq!(handoffs.len(), 2);
        assert_eq!(handoffs[0].chunk, chunk_a);
        assert_eq!(handoffs[0].entity_local_ids, vec![2, 3]);
        assert_eq!(handoffs[0].targets, vec![2, 3]);
        assert_eq!(handoffs[1].chunk, chunk_b);
        assert!(handoffs[1].targets.is_empty());
    }

    #[test]
    fn sendable_effects_wrap_as_handshakes() {
        let effect = LifecycleEffect::SendVote {
            to: vec![4],
            vote: JoinVote {
                candidate: 4,
                voter: 1,
                approve: true,
            },
        };
        assert!(effect.handshake().is_some());
        assert!(LifecycleEffect::Joined { candidate: 4 }.handshake().is_none());
    }

    #[tokio::test]
    async fn driver_pumps_inputs_to_effects() {
        let (input_tx, mut input_rx) = mpsc::channel(8);
        let (effect_tx, mut effect_rx) = mpsc::channel(8);
        let node = member_node(1);
        let driver = tokio::spawn(async move {
            run_lifecycle(node, &mut input_rx, &effect_tx).await;
        });
        input_tx
            .send(LifecycleInput::NoteUncleanDrop { peer: 3 })
            .await
            .unwrap();
        drop(input_tx);
        driver.await.unwrap();
        let mut saw_suspect = false;
        while let Ok(effect) = effect_rx.try_recv() {
            if matches!(effect, LifecycleEffect::SuspectMarked { peer: 3 }) {
                saw_suspect = true;
            }
        }
        assert!(saw_suspect);
    }
}
