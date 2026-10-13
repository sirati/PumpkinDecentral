use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::identity::GlobalPlayerId;
use crate::presence::{PresenceLogin, PresenceLogout};
use crate::realm_completion::PeerGeneration;

pub const PREFIX: &[u8; 8] = b"PrsOrd01";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PresencePayload {
    Profile {
        login: PresenceLogin,
        hidden: bool,
    },
    Hide {
        gid: GlobalPlayerId,
        name: String,
        hidden: bool,
    },
    Logout(PresenceLogout),
    Retired {
        gid: GlobalPlayerId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedPresence {
    pub source: PeerGeneration,
    pub session: u64,
    pub uuid: [u8; 16],
    pub revision: u64,
    pub payload: PresencePayload,
}

impl OrderedPresence {
    pub fn gid(&self) -> GlobalPlayerId {
        match &self.payload {
            PresencePayload::Profile { login, .. } => login.gid,
            PresencePayload::Hide { gid, .. } | PresencePayload::Retired { gid } => *gid,
            PresencePayload::Logout(logout) => logout.gid,
        }
    }

    pub fn valid(&self) -> bool {
        self.source.generation != 0
            && self.session != 0
            && self.revision != 0
            && self.gid().server == self.source.server
            && match &self.payload {
                PresencePayload::Profile { login, .. } => {
                    login.uuid == self.uuid && crate::presence::is_valid_presence_name(&login.name)
                }
                PresencePayload::Hide { name, .. } => crate::presence::is_valid_presence_name(name),
                PresencePayload::Logout(logout) => {
                    !matches!(&logout.announcement, crate::presence::DepartureAnnouncement::CustomJson(json) if json.len() > 16384)
                }
                PresencePayload::Retired { .. } => true,
            }
    }
}

#[derive(Clone, Default)]
struct OrderedState {
    source: Option<PeerGeneration>,
    session: u64,
    uuid: [u8; 16],
    latest: u64,
    profile_revision: u64,
    hide_revision: u64,
    retired_revision: u64,
    profile: Option<OrderedPresence>,
    hide: Option<OrderedPresence>,
}

#[derive(Clone, Default)]
pub struct PresenceOrdering {
    states: BTreeMap<GlobalPlayerId, OrderedState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PresenceDisposition {
    Profile { hidden: Option<bool> },
    Hide,
    Retired,
    Ignored,
}

impl PresenceOrdering {
    pub fn issue(
        &self,
        source: PeerGeneration,
        session: u64,
        uuid: [u8; 16],
        payload: PresencePayload,
    ) -> Option<OrderedPresence> {
        let mut envelope = OrderedPresence {
            source,
            session,
            uuid,
            revision: 1,
            payload,
        };
        if let Some(state) = self
            .states
            .get(&envelope.gid())
            .filter(|state| state.source == Some(source))
        {
            if session < state.session
                || session == state.session && (uuid != state.uuid || state.retired_revision != 0)
            {
                return None;
            }
            envelope.revision = state.latest.checked_add(1)?;
        }
        envelope.valid().then_some(envelope)
    }

    pub fn observe(&mut self, envelope: &OrderedPresence) -> PresenceDisposition {
        if !envelope.valid() {
            return PresenceDisposition::Ignored;
        }
        let state = self.states.entry(envelope.gid()).or_default();
        if state.source != Some(envelope.source) {
            *state = OrderedState {
                source: Some(envelope.source),
                ..Default::default()
            };
        }
        if envelope.session < state.session
            || envelope.session == state.session
                && state.session != 0
                && envelope.uuid != state.uuid
            || envelope.session > state.session && envelope.revision <= state.latest
        {
            return PresenceDisposition::Ignored;
        }
        if envelope.session > state.session {
            let latest = state.latest;
            *state = OrderedState {
                source: Some(envelope.source),
                session: envelope.session,
                uuid: envelope.uuid,
                latest,
                ..Default::default()
            };
        }
        let disposition = match &envelope.payload {
            PresencePayload::Profile { hidden, .. }
                if envelope.revision > state.profile_revision
                    && envelope.revision > state.retired_revision =>
            {
                state.profile_revision = envelope.revision;
                state.profile = Some(envelope.clone());
                let hidden = if envelope.revision > state.hide_revision {
                    state.hide_revision = envelope.revision;
                    state.hide = None;
                    Some(*hidden)
                } else {
                    None
                };
                PresenceDisposition::Profile { hidden }
            }
            PresencePayload::Hide { .. }
                if envelope.revision > state.hide_revision
                    && envelope.revision > state.retired_revision =>
            {
                state.hide_revision = envelope.revision;
                state.hide = Some(envelope.clone());
                PresenceDisposition::Hide
            }
            PresencePayload::Logout(_) | PresencePayload::Retired { .. }
                if envelope.revision > state.latest =>
            {
                state.retired_revision = envelope.revision;
                state.profile = None;
                state.hide = None;
                PresenceDisposition::Retired
            }
            _ => PresenceDisposition::Ignored,
        };
        if disposition != PresenceDisposition::Ignored {
            state.latest = state.latest.max(envelope.revision);
        }
        disposition
    }

    pub fn profile(&self, gid: GlobalPlayerId) -> Option<&OrderedPresence> {
        self.states
            .get(&gid)
            .and_then(|state| state.profile.as_ref())
    }

    pub fn session(&self, gid: GlobalPlayerId) -> Option<u64> {
        self.states
            .get(&gid)
            .filter(|state| state.retired_revision == 0)
            .map(|state| state.session)
    }

    pub fn snapshots(&self) -> Vec<OrderedPresence> {
        let mut result = Vec::new();
        for (gid, state) in &self.states {
            if state.retired_revision != 0 {
                if let Some(source) = state.source {
                    result.push(OrderedPresence {
                        source,
                        session: state.session,
                        uuid: state.uuid,
                        revision: state.retired_revision,
                        payload: PresencePayload::Retired { gid: *gid },
                    });
                }
            } else {
                result.extend(state.profile.iter().cloned());
                result.extend(state.hide.iter().cloned());
            }
        }
        result
    }

    pub fn len(&self) -> usize {
        self.states.len()
    }
}

pub fn encode(envelope: &OrderedPresence) -> Option<Vec<u8>> {
    if !envelope.valid() {
        return None;
    }
    let payload = postcard::to_allocvec(envelope).ok()?;
    if payload.len() > 131_072 {
        return None;
    }
    let mut bytes = PREFIX.to_vec();
    bytes.extend_from_slice(&payload);
    Some(bytes)
}

pub fn decode(bytes: &[u8]) -> Option<OrderedPresence> {
    let payload = bytes
        .strip_prefix(PREFIX)
        .filter(|payload| payload.len() <= 131_072)?;
    let envelope: OrderedPresence = postcard::from_bytes(payload).ok()?;
    envelope.valid().then_some(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn source() -> PeerGeneration {
        PeerGeneration {
            server: ServerId(2),
            generation: 102,
        }
    }
    fn login(uuid: [u8; 16]) -> PresenceLogin {
        PresenceLogin::new(
            GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            uuid,
            "Alice".into(),
            vec![],
            true,
        )
    }
    fn issue(
        state: &mut PresenceOrdering,
        session: u64,
        uuid: [u8; 16],
        payload: PresencePayload,
    ) -> OrderedPresence {
        let envelope = state.issue(source(), session, uuid, payload).unwrap();
        assert_ne!(state.observe(&envelope), PresenceDisposition::Ignored);
        envelope
    }

    #[test]
    fn reordered_profile_and_hide_preserve_independent_facets_and_original_seed() {
        let mut issuer = PresenceOrdering::default();
        let profile = issue(
            &mut issuer,
            17,
            [7; 16],
            PresencePayload::Profile {
                login: login([7; 16]),
                hidden: false,
            },
        );
        let hide = issue(
            &mut issuer,
            17,
            [7; 16],
            PresencePayload::Hide {
                gid: profile.gid(),
                name: "Alice".into(),
                hidden: true,
            },
        );
        let mut receiving = PresenceOrdering::default();
        assert_eq!(receiving.observe(&hide), PresenceDisposition::Hide);
        assert_eq!(
            receiving.observe(&profile),
            PresenceDisposition::Profile { hidden: None }
        );
        assert_eq!(receiving.snapshots(), vec![profile.clone(), hide.clone()]);
        assert_eq!(receiving.profile(profile.gid()), Some(&profile));
        assert_eq!(receiving.observe(&profile), PresenceDisposition::Ignored);
        assert_eq!(receiving.observe(&hide), PresenceDisposition::Ignored);
        let roundtrip = decode(&encode(&hide).unwrap()).unwrap();
        assert_eq!(roundtrip.source, source());
        assert_eq!(roundtrip.session, 17);
        assert_eq!(roundtrip.revision, hide.revision);
    }

    #[test]
    fn retirement_seed_prevents_late_profile_and_only_new_session_can_reopen() {
        let mut issuer = PresenceOrdering::default();
        let profile = issue(
            &mut issuer,
            17,
            [7; 16],
            PresencePayload::Profile {
                login: login([7; 16]),
                hidden: false,
            },
        );
        let logout = issue(
            &mut issuer,
            17,
            [7; 16],
            PresencePayload::Logout(PresenceLogout::new(profile.gid())),
        );
        let seed = issuer.snapshots();
        assert_eq!(seed.len(), 1);
        assert!(matches!(seed[0].payload, PresencePayload::Retired { .. }));
        assert_eq!(seed[0].source, profile.source);
        assert_eq!(seed[0].uuid, profile.uuid);
        assert_eq!(seed[0].session, profile.session);
        assert_eq!(seed[0].revision, logout.revision);
        let mut restarted = PresenceOrdering::default();
        assert_eq!(restarted.observe(&seed[0]), PresenceDisposition::Retired);
        assert_eq!(restarted.observe(&profile), PresenceDisposition::Ignored);
        assert!(restarted.profile(profile.gid()).is_none());
        assert!(
            restarted
                .issue(source(), 17, [7; 16], profile.payload.clone())
                .is_none()
        );
        let replacement = restarted
            .issue(source(), 18, [7; 16], profile.payload.clone())
            .unwrap();
        assert!(replacement.revision > logout.revision);
        assert_eq!(
            restarted.observe(&replacement),
            PresenceDisposition::Profile {
                hidden: Some(false)
            }
        );
        assert_eq!(restarted.session(profile.gid()), Some(18));
        assert_eq!(restarted.observe(&logout), PresenceDisposition::Ignored);
    }

    #[test]
    fn reused_gid_rejects_prior_session_even_with_larger_revision_or_reused_uuid() {
        for replacement_uuid in [[7; 16], [8; 16]] {
            let mut state = PresenceOrdering::default();
            let profile = issue(
                &mut state,
                17,
                [7; 16],
                PresencePayload::Profile {
                    login: login([7; 16]),
                    hidden: false,
                },
            );
            let retired = issue(
                &mut state,
                17,
                [7; 16],
                PresencePayload::Logout(PresenceLogout::new(profile.gid())),
            );
            let replacement = issue(
                &mut state,
                18,
                replacement_uuid,
                PresencePayload::Profile {
                    login: login(replacement_uuid),
                    hidden: false,
                },
            );
            let mut late = profile;
            late.revision = replacement.revision + 100;
            assert_eq!(state.observe(&late), PresenceDisposition::Ignored);
            assert_eq!(state.observe(&retired), PresenceDisposition::Ignored);
            assert_eq!(state.profile(replacement.gid()), Some(&replacement));
            let mut same_session_other_uuid = replacement.clone();
            same_session_other_uuid.uuid = [9; 16];
            if let PresencePayload::Profile { login, .. } = &mut same_session_other_uuid.payload {
                login.uuid = [9; 16];
            }
            same_session_other_uuid.revision += 1;
            assert_eq!(
                state.observe(&same_session_other_uuid),
                PresenceDisposition::Ignored
            );
        }
    }
}
