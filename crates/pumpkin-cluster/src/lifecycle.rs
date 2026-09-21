use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::membership::{
    AdmitBroadcast, JoinHandshake, JoinHello, JoinVote, JoinVoteRequest, JoinVotes, Membership,
    can_leave_network, handshake_channel,
};
use crate::entities::{EntityHandoff, EntityOrigin, EntitySpawn};
use crate::identity::ServerId;
use crate::protocol::ChunkAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    Solo,
    Joining,
    Member,
    Leaving,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedEntity {
    pub spawn: EntitySpawn,
    pub velocity: [f64; 3],
}

impl OwnedEntity {
    #[must_use]
    pub fn origin(&self) -> EntityOrigin {
        EntityOrigin {
            server: self.spawn.entity.origin,
            local_id: self.spawn.entity.local_id,
        }
    }

    #[must_use]
    pub const fn chunk(&self) -> ChunkAddr {
        self.spawn.entity.chunk
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityHandoffRequest {
    pub leaver: u16,
    pub handoffs: Vec<EntityHandoff>,
}

impl EntityHandoffRequest {
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        self.handoffs.iter().all(|handoff| {
            handoff.previous_owner.0 == self.leaver && handoff.is_consistent()
        })
    }

    #[must_use]
    pub fn for_successor(&self, successor: ServerId) -> Self {
        Self {
            leaver: self.leaver,
            handoffs: self
                .handoffs
                .iter()
                .filter(|handoff| handoff.applies_to(successor))
                .cloned()
                .collect(),
        }
    }

    #[must_use]
    pub fn successors(&self) -> Vec<ServerId> {
        let mut successors: Vec<ServerId> = self
            .handoffs
            .iter()
            .map(|handoff| handoff.successor)
            .collect();
        successors.sort_unstable();
        successors.dedup();
        successors
    }
}

#[must_use]
pub fn plan_handoff(
    owned: &[OwnedEntity],
    holders: &dyn Fn(ChunkAddr) -> Vec<u16>,
) -> Option<Vec<EntityHandoff>> {
    let mut handoffs = Vec::with_capacity(owned.len());
    for entity in owned {
        let mut targets = holders(entity.chunk());
        targets.sort_unstable();
        targets.dedup();
        let successor = targets
            .into_iter()
            .find(|target| *target != entity.spawn.entity.owner.0)
            .map(ServerId)?;
        let handoff = EntityHandoff {
            origin: entity.origin(),
            previous_owner: entity.spawn.entity.owner,
            successor,
            spawn: entity.spawn.clone(),
            velocity: entity.velocity,
        };
        if !handoff.is_consistent() {
            return None;
        }
        handoffs.push(handoff);
    }
    handoffs.sort_by(|left, right| {
        (left.origin.server, left.origin.local_id)
            .cmp(&(right.origin.server, right.origin.local_id))
    });
    Some(handoffs)
}

#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleInput {
    BeginJoin {
        cert_fingerprint: [u8; 32],
        peers: Vec<u16>,
    },
    PeerReady {
        peer: u16,
    },
    RemoteHello {
        hello: JoinHello,
        pin_match: bool,
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
    LeaveNoticeEnqueued,
    NoteUncleanDrop {
        peer: u16,
    },
    OperatorClearSuspect {
        peer: u16,
    },
    CompleteLeave,
}

#[derive(Debug, Clone, PartialEq)]
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
    configured_voters: HashSet<u16>,
    suspects: HashSet<u16>,
    hellos: HashMap<u16, JoinHello>,
    ready_peers: HashSet<u16>,
    hello_sent_to: HashSet<u16>,
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
            configured_voters: HashSet::new(),
            suspects: HashSet::new(),
            hellos: HashMap::new(),
            ready_peers: HashSet::new(),
            hello_sent_to: HashSet::new(),
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
            configured_voters: HashSet::new(),
            suspects: HashSet::new(),
            hellos: HashMap::new(),
            ready_peers: HashSet::new(),
            hello_sent_to: HashSet::new(),
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
    pub fn player_join_allowed(&self) -> bool {
        self.state == LifecycleState::Member
    }

    pub fn apply(&mut self, input: LifecycleInput) -> Vec<LifecycleEffect> {
        match input {
            LifecycleInput::BeginJoin {
                cert_fingerprint,
                peers,
            } => self.begin_join(cert_fingerprint, peers),
            LifecycleInput::PeerReady { peer } => self.peer_ready(peer),
            LifecycleInput::RemoteHello { hello, pin_match } => {
                self.remote_hello(hello, pin_match)
            }
            LifecycleInput::RemoteVoteRequest(request) => self.remote_vote_request(request),
            LifecycleInput::RemoteVote(vote) => self.remote_vote(vote),
            LifecycleInput::RemoteAdmit(admit) => self.remote_admit(admit),
            LifecycleInput::RemoteHandoff(request) => self.remote_handoff(request),
            LifecycleInput::RequestLeave {
                player_count,
                owned,
                holders,
            } => self.request_leave(player_count, &owned, &holders),
            LifecycleInput::LeaveNoticeEnqueued => self.leave_notice_enqueued(),
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

    fn knows_candidate(&self, candidate: u16) -> bool {
        candidate == self.local_id || self.hellos.contains_key(&candidate)
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
        let mut to = self.voters_for(candidate);
        to.retain(|peer| *peer != self.local_id);
        if !to.contains(&candidate) {
            to.push(candidate);
        }
        to.sort_unstable();
        to
    }

    fn voters_for(&self, candidate: u16) -> Vec<u16> {
        let mut voters: Vec<u16> = if self.configured_voters.contains(&candidate) {
            self.configured_voters.iter().copied().collect()
        } else {
            self.membership.members.iter().copied().collect()
        };
        voters.sort_unstable();
        voters
    }

    fn voter_is_expected(&self, candidate: u16, voter: u16) -> bool {
        self.voters_for(candidate).contains(&voter)
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
        let approved = self
            .votes
            .votes
            .get(&candidate)
            .is_some_and(|voters| self.voters_for(candidate).iter().all(|voter| voters.contains(voter)));
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
        self.configured_voters.clear();
        self.configured_voters.insert(self.local_id);
        self.configured_voters.extend(to.iter().copied());
        self.hello_sent_to.clear();
        self.set_state(LifecycleState::Joining, &mut effects);
        let ready: Vec<u16> = self.ready_peers.iter().copied().collect();
        for peer in ready {
            self.send_hello_to_ready_peer(peer, &mut effects);
        }
        self.votes
            .record(&self.membership, self.local_id, self.local_id);
        self.check_unanimity(self.local_id, &mut effects);
        effects
    }

    fn peer_ready(&mut self, peer: u16) -> Vec<LifecycleEffect> {
        self.ready_peers.insert(peer);
        self.hello_sent_to.remove(&peer);
        let mut effects = Vec::new();
        self.send_hello_to_ready_peer(peer, &mut effects);
        effects
    }

    fn send_hello_to_ready_peer(&mut self, peer: u16, effects: &mut Vec<LifecycleEffect>) {
        if self.state != LifecycleState::Joining
            || peer == self.local_id
            || !self.configured_voters.contains(&peer)
            || !self.ready_peers.contains(&peer)
            || !self.hello_sent_to.insert(peer)
        {
            return;
        }
        effects.push(LifecycleEffect::SendHello {
            to: vec![peer],
            hello: JoinHello {
                candidate: self.local_id,
                cert_fingerprint: self.local_fingerprint,
            },
        });
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
        if candidate == self.local_id
            || self.membership.contains(candidate)
            || !self.knows_candidate(candidate)
        {
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
        if self.is_admitted(candidate) || !self.knows_candidate(candidate) {
            return effects;
        }
        if self.suspects.contains(&candidate) {
            effects.push(LifecycleEffect::VoteBlocked { candidate });
            return effects;
        }
        if !self.voter_is_expected(candidate, vote.voter) {
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
        if !self.knows_candidate(candidate) {
            return effects;
        }
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
        if leaver == self.local_id
            || !self.membership.contains(leaver)
            || !request.handoffs.is_empty()
            || !request.is_consistent()
        {
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
            return effects;
        }
        if player_count == 0 {
            let lookup = |chunk: ChunkAddr| {
                holders
                    .iter()
                    .find(|entry| entry.0 == chunk)
                    .map_or_else(Vec::new, |entry| entry.1.clone())
            };
            if let Some(handoffs) = plan_handoff(owned, &lookup) {
                effects.push(LifecycleEffect::EmitHandoff(EntityHandoffRequest {
                    leaver: self.local_id,
                    handoffs,
                }));
            }
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
        self.suspects.insert(peer);
        vec![LifecycleEffect::SuspectMarked { peer }]
    }

    fn operator_clear_suspect(&mut self, peer: u16) -> Vec<LifecycleEffect> {
        if self.suspects.remove(&peer) {
            vec![LifecycleEffect::SuspectCleared { peer }]
        } else {
            Vec::new()
        }
    }

    fn leave_notice_enqueued(&mut self) -> Vec<LifecycleEffect> {
        let mut effects = Vec::new();
        if self.state == LifecycleState::Member {
            self.set_state(LifecycleState::Leaving, &mut effects);
        }
        effects
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

pub async fn run_lifecycle(
    mut lifecycle: Lifecycle,
    inputs: &mut mpsc::Receiver<LifecycleInput>,
    effects: &mpsc::Sender<LifecycleEffect>,
) {
    while let Some(input) = inputs.recv().await {
        let out = lifecycle.apply(input);
        for effect in out {
            if effects.try_send(effect).is_err() {
                tracing::warn!("cluster lifecycle effect dropped: output queue unavailable");
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

    fn owned(local_id: i32, chunk: ChunkAddr) -> OwnedEntity {
        OwnedEntity {
            spawn: EntitySpawn {
                entity: crate::protocol::EntityRef {
                    origin: ServerId(1),
                    owner: ServerId(1),
                    local_id,
                    chunk,
                },
                tick: crate::time::TickStamp(9),
                kind: 1,
                pos: [0.0, 64.0, 0.0],
                yaw: 0.0,
                pitch: 0.0,
                state: crate::entities::EntitySpawnState::Entity {
                    nbt: vec![10, 0, 0],
                },
            },
            velocity: [0.0, 0.0, 0.0],
        }
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
        let owned = vec![owned(11, chunk_a), owned(12, chunk_a), owned(13, chunk_b)];
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
        assert_eq!(request.handoffs.len(), 3);
        assert_eq!(request.handoffs[0].successor, ServerId(2));
        assert_eq!(request.handoffs[1].successor, ServerId(2));
        assert_eq!(request.handoffs[2].successor, ServerId(3));
        assert!(request.is_consistent());
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::LeaveBlocked { owned_entities: 3, .. }))
        );
        assert_eq!(node.state(), LifecycleState::Member);
    }

    #[test]
    fn leave_handoff_is_scoped_to_its_successor() {
        let mut node = member_node(1);
        let chunk = ChunkAddr { x: 0, z: 0 };
        let effects = node.apply(LifecycleInput::RequestLeave {
            player_count: 0,
            owned: vec![owned(11, chunk), owned(12, chunk)],
            holders: vec![(chunk, vec![1, 2, 3])],
        });
        let request = effects
            .iter()
            .find_map(|effect| match effect {
                LifecycleEffect::EmitHandoff(request) => Some(request),
                _ => None,
            })
            .expect("leave emits transfer");
        assert_eq!(request.successors(), vec![ServerId(2)]);
        let scoped = request.for_successor(ServerId(2));
        assert_eq!(scoped.handoffs.len(), 2);
        assert!(scoped.handoffs.iter().all(|handoff| {
            handoff.applies_to(ServerId(2))
                && handoff.destination_ref().owner == ServerId(2)
                && handoff.destination_ref().origin == ServerId(1)
        }));
        assert!(request.for_successor(ServerId(3)).handoffs.is_empty());
    }

    #[test]
    fn leave_does_not_emit_a_partial_handoff() {
        let mut node = member_node(1);
        let available = ChunkAddr { x: 0, z: 0 };
        let unavailable = ChunkAddr { x: 1, z: 0 };
        let effects = node.apply(LifecycleInput::RequestLeave {
            player_count: 0,
            owned: vec![owned(11, available), owned(12, unavailable)],
            holders: vec![(available, vec![1, 2]), (unavailable, vec![1])],
        });
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, LifecycleEffect::EmitHandoff(_))));
        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                LifecycleEffect::LeaveBlocked {
                    player_count: 0,
                    owned_entities: 2
                }
            )
        }));
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
        assert_eq!(node.state(), LifecycleState::Member);
        assert!(effects
            .iter()
            .any(|effect| matches!(effect, LifecycleEffect::EmitHandoff(request) if request.handoffs.is_empty())));
        let effects = node.apply(LifecycleInput::LeaveNoticeEnqueued);
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
    fn suspect_marks_without_removing_member() {
        let mut node = member_node(1);
        let effects = node.apply(LifecycleInput::NoteUncleanDrop { peer: 2 });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::SuspectMarked { peer: 2 }))
        );
        assert!(node.membership().contains(2));
        assert!(node.is_suspect(2));

        let effects = node.apply(LifecycleInput::OperatorClearSuspect { peer: 2 });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, LifecycleEffect::SuspectCleared { peer: 2 }))
        );
        assert!(!node.is_suspect(2));
        assert!(node.membership().contains(2));
    }

    #[test]
    fn candidate_waits_for_configured_peer_vote() {
        let mut node = Lifecycle::new_solo(5);
        let effects = node.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![6],
        });
        assert_eq!(node.state(), LifecycleState::Joining);
        assert!(!joined(&effects, 5));
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, LifecycleEffect::SendHello { .. })));
        let effects = node.apply(LifecycleInput::PeerReady { peer: 6 });
        assert!(
            effects.iter().any(
                |effect| matches!(effect, LifecycleEffect::SendHello { to, .. } if to == &vec![6_u16])
            )
        );
        let effects = node.apply(vote(5, 6));
        assert_eq!(node.state(), LifecycleState::Member);
        assert!(joined(&effects, 5));
    }

    #[test]
    fn ready_peer_before_begin_join_gets_hello() {
        let mut node = Lifecycle::new_solo(5);
        assert!(node
            .apply(LifecycleInput::PeerReady { peer: 6 })
            .is_empty());
        let effects = node.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![6],
        });
        assert!(effects.iter().any(
            |effect| matches!(effect, LifecycleEffect::SendHello { to, .. } if to == &vec![6_u16])
        ));
    }

    #[test]
    fn bootstrap_voters_admit_every_configured_peer() {
        let mut node = Lifecycle::new_solo(1);
        node.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![2, 3],
        });
        let votes = node.apply(hello(2));
        assert!(votes.iter().any(|effect| {
            matches!(effect, LifecycleEffect::SendVote { to, vote } if vote.candidate == 2 && to == &vec![2, 3])
        }));
        node.apply(hello(3));
        node.apply(vote(1, 2));
        node.apply(vote(1, 3));
        node.apply(vote(2, 2));
        node.apply(vote(2, 3));
        node.apply(vote(3, 2));
        node.apply(vote(3, 3));
        assert_eq!(node.state(), LifecycleState::Member);
        assert!(node.membership().contains(1));
        assert!(node.membership().contains(2));
        assert!(node.membership().contains(3));
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
    fn player_joins_allowed_only_after_admit() {
        assert!(!Lifecycle::new_solo(1).player_join_allowed());
        assert!(Lifecycle::new_with_members(1, &[1, 2]).player_join_allowed());
        let mut joining = Lifecycle::new_solo(9);
        joining.apply(LifecycleInput::BeginJoin {
            cert_fingerprint: FINGERPRINT,
            peers: vec![1, 2],
        });
        assert!(!joining.player_join_allowed());
        let mut leaving = member_node(1);
        leaving.apply(LifecycleInput::RequestLeave {
            player_count: 0,
            owned: Vec::new(),
            holders: Vec::new(),
        });
        leaving.apply(LifecycleInput::LeaveNoticeEnqueued);
        assert!(!leaving.player_join_allowed());
    }

    #[test]
    fn suspect_mark_keeps_membership_and_blocks_pending_join() {
        let mut node = member_node(1);
        node.apply(hello(9));
        let effects = node.apply(vote(9, 2));
        assert!(!joined(&effects, 9));
        let effects = node.apply(LifecycleInput::NoteUncleanDrop { peer: 3 });
        assert!(!joined(&effects, 9));
        assert!(node.membership().contains(3));
        assert!(!node.membership().contains(9));
        node.apply(LifecycleInput::OperatorClearSuspect { peer: 3 });
        let effects = node.apply(vote(9, 3));
        assert!(joined(&effects, 9));
    }

    #[test]
    fn plan_handoff_groups_per_chunk() {
        let chunk_a = ChunkAddr { x: 0, z: 0 };
        let chunk_b = ChunkAddr { x: 0, z: 1 };
        let owned = vec![owned(1, chunk_b), owned(2, chunk_a), owned(3, chunk_a)];
        let handoffs = plan_handoff(&owned, &|chunk| {
            if chunk == chunk_a {
                vec![2_u16, 3_u16]
            } else {
                Vec::new()
            }
        });
        assert!(handoffs.is_none());
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
