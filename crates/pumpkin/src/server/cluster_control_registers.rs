use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Stamp {
    pub millis: u64,
    pub origin: u16,
    pub incarnation: u64,
    pub ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Register {
    pub key: String,
    pub stamp: Stamp,
    pub value: Option<serde_json::Value>,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Registers {
    entries: BTreeMap<String, Register>,
    clock: u64,
}

impl Registers {
    pub fn issue(
        &mut self,
        origin: u16,
        incarnation: u64,
        key: String,
        value: Option<serde_json::Value>,
    ) -> Register {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        self.clock = self
            .clock
            .checked_add(1)
            .expect("global control clock exhausted")
            .max(now);
        Register {
            key,
            value,
            stamp: Stamp {
                millis: self.clock,
                origin,
                incarnation,
                ordinal: self.clock,
            },
        }
    }

    pub fn merge(&mut self, register: Register) -> bool {
        self.clock = self.clock.max(register.stamp.millis);
        if self
            .entries
            .get(&register.key)
            .is_some_and(|old| old.stamp >= register.stamp)
        {
            return false;
        }
        self.entries.insert(register.key.clone(), register);
        true
    }

    pub fn merge_authenticated(
        &mut self,
        origin: u16,
        incarnation: u64,
        entries: Vec<Register>,
        valid: impl Fn(&Register) -> bool,
    ) -> Option<bool> {
        if !entries
            .iter()
            .all(|entry| entry.stamp.origin == origin && entry.stamp.incarnation == incarnation)
        {
            return None;
        }
        self.merge_reconciled(entries, valid)
    }

    pub fn merge_reconciled(
        &mut self,
        entries: Vec<Register>,
        valid: impl Fn(&Register) -> bool,
    ) -> Option<bool> {
        let mut seen = BTreeMap::new();
        for entry in &entries {
            if !valid(entry)
                || self
                    .entries
                    .get(&entry.key)
                    .is_some_and(|old| old.stamp == entry.stamp && old != entry)
            {
                return None;
            }
            if let Some(previous) = seen.insert((&entry.key, &entry.stamp), &entry.value)
                && previous != &entry.value
            {
                return None;
            }
        }
        drop(seen);
        let mut changed = false;
        for entry in entries {
            changed |= self.merge(entry);
        }
        Some(changed)
    }

    pub fn entries(&self) -> impl Iterator<Item = &Register> {
        self.entries.values()
    }
}

#[derive(Default)]
pub struct Diagnostics {
    last: BTreeMap<(u16, &'static str), (std::time::Instant, u64)>,
    untracked_suppressed: u64,
}

impl Diagnostics {
    fn eligible(&mut self, peer: u16, reason: &'static str) -> bool {
        let key = (peer, reason);
        let now = std::time::Instant::now();
        if let Some((last, suppressed)) = self.last.get_mut(&key) {
            if now.duration_since(*last).as_secs() < 300 {
                *suppressed = suppressed.saturating_add(1);
                return false;
            }
            *last = now;
        } else if self.last.len() >= 1024 {
            self.untracked_suppressed = self.untracked_suppressed.saturating_add(1);
            return false;
        } else {
            self.last.insert(key, (now, 0));
        }
        true
    }

    pub fn suppressed_count(&self) -> u64 {
        self.last
            .values()
            .fold(self.untracked_suppressed, |total, (_, count)| {
                total.saturating_add(*count)
            })
    }

    pub fn rejected(&mut self, domain: &'static str, peer: u16, reason: &'static str) {
        if self.eligible(peer, reason) {
            tracing::warn!(
                domain,
                peer,
                reason,
                stream = "Control",
                suppressed = self.suppressed_count(),
                "global control rejected"
            );
        }
    }

    pub fn deferred(&mut self, domain: &'static str, peer: u16, reason: &'static str) {
        if self.eligible(peer, reason) {
            tracing::warn!(
                domain,
                peer,
                reason,
                stream = "Control",
                suppressed = self.suppressed_count(),
                "global control retained pending readiness"
            );
        }
    }
}

pub fn control_publication_ready(received: u64, current: Option<u64>, boot: Option<u64>) -> bool {
    current.is_some_and(|current| {
        current > received || current == received && boot.is_some_and(|boot| boot != 0)
    })
}

pub fn take_ready_control<T>(
    pending: &mut std::collections::VecDeque<T>,
    ready: impl Fn(&T) -> bool,
) -> Option<T> {
    let index = pending.iter().position(ready)?;
    pending.remove(index)
}

#[derive(Default)]
pub struct SeedAssemblies {
    parts: BTreeMap<(u16, u64, [u8; 16]), Vec<Option<Vec<Register>>>>,
}

pub enum SeedResult {
    Pending,
    Full,
    Complete(Vec<Register>),
    Rejected(&'static str),
}

impl SeedAssemblies {
    pub fn can_accept(&self, peer: u16, generation: u64, transfer: [u8; 16]) -> bool {
        self.parts.len() < 64 || self.parts.contains_key(&(peer, generation, transfer))
    }

    pub fn retain(&mut self, current: impl Fn(u16, u64) -> bool) {
        self.parts
            .retain(|(peer, generation, _), _| current(*peer, *generation));
    }

    pub fn push(
        &mut self,
        peer: u16,
        generation: u64,
        transfer: [u8; 16],
        index: u16,
        total: u16,
        entries: Vec<Register>,
    ) -> SeedResult {
        if total == 0 || total > 64 || index >= total {
            return SeedResult::Rejected("invalid seed bounds");
        }
        let key = (peer, generation, transfer);
        if !self.can_accept(peer, generation, transfer) {
            return SeedResult::Full;
        }
        let parts = self
            .parts
            .entry(key)
            .or_insert_with(|| vec![None; usize::from(total)]);
        if parts.len() != usize::from(total)
            || parts[usize::from(index)]
                .as_ref()
                .is_some_and(|old| old != &entries)
        {
            self.parts.remove(&key);
            return SeedResult::Rejected("contradictory seed part");
        }
        parts[usize::from(index)] = Some(entries);
        if parts.iter().all(Option::is_some) {
            return SeedResult::Complete(
                self.parts
                    .remove(&key)
                    .unwrap()
                    .into_iter()
                    .flatten()
                    .flatten()
                    .collect(),
            );
        }
        SeedResult::Pending
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum Frame {
    Reload,
    Mutation(Vec<Register>),
    Seed {
        transfer: [u8; 16],
        index: u16,
        total: u16,
        entries: Vec<Register>,
    },
}

pub fn encode(prefix: &[u8; 8], frame: &Frame) -> Option<Vec<u8>> {
    let payload = serde_json::to_vec(frame).ok()?;
    if payload.len() > 900_000 {
        return None;
    }
    let mut bytes = prefix.to_vec();
    bytes.extend(payload);
    Some(bytes)
}

pub fn decode(prefix: &[u8; 8], bytes: &[u8]) -> Option<Frame> {
    if bytes.len() > 900_008 {
        return None;
    }
    serde_json::from_slice(bytes.strip_prefix(prefix)?).ok()
}

pub fn seed_frames(prefix: &[u8; 8], registers: &Registers) -> Option<Vec<Vec<u8>>> {
    let mut groups = Vec::new();
    let mut group = Vec::new();
    let mut size = 0;
    for entry in registers.entries() {
        let length = serde_json::to_vec(entry).ok()?.len();
        if length > 800_000 {
            return None;
        }
        if size + length > 800_000 {
            groups.push(std::mem::take(&mut group));
            size = 0;
        }
        group.push(entry.clone());
        size += length;
    }
    groups.push(group);
    let total = u16::try_from(groups.len()).ok()?;
    if total > 64 {
        return None;
    }
    let transfer = *uuid::Uuid::new_v4().as_bytes();
    groups
        .into_iter()
        .enumerate()
        .map(|(index, entries)| {
            encode(
                prefix,
                &Frame::Seed {
                    transfer,
                    index: index as u16,
                    total,
                    entries,
                },
            )
        })
        .collect()
}

pub fn save(path: &std::path::Path, registers: &Registers) -> std::io::Result<()> {
    use std::io::Write;
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing global control directory"))?;
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join(format!(".global-control-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(registers).map_err(std::io::Error::other)?)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reorder_restart_and_delayed_grant_preserve_policy_removal() {
        let mut issuer = Registers::default();
        let grant = issuer.issue(2, 11, "whitelist/alice".into(), Some("Alice".into()));
        issuer.merge(grant.clone());
        let revoke = issuer.issue(2, 11, "whitelist/alice".into(), None);
        let unrelated = issuer.issue(3, 12, "whitelist/bob".into(), Some("Bob".into()));
        let mut first = Registers::default();
        let mut second = Registers::default();
        for value in [grant.clone(), revoke.clone(), unrelated.clone()] {
            first.merge(value);
        }
        for value in [unrelated, revoke, grant.clone()] {
            second.merge(value);
        }
        assert_eq!(first.entries, second.entries);
        let bytes = serde_json::to_vec(&first).unwrap();
        let mut restarted: Registers = serde_json::from_slice(&bytes).unwrap();
        assert!(!restarted.merge(grant));
        assert!(restarted.entries["whitelist/alice"].value.is_none());
        assert_eq!(restarted.entries["whitelist/bob"].value, Some("Bob".into()));
        let next = restarted.issue(2, 99, "whitelist/alice".into(), Some("Alice".into()));
        assert!(restarted.merge(next));
        assert_eq!(
            restarted.entries["whitelist/alice"].value,
            Some("Alice".into())
        );
    }

    #[test]
    fn persisted_tombstone_survives_replacement_and_late_pre_restart_mutation() {
        let directory =
            std::env::temp_dir().join(format!("pumpkin-controls-{}", uuid::Uuid::new_v4()));
        let path = directory.join("state.json");
        let mut state = Registers::default();
        let original = state.issue(2, 11, "member/alice".into(), Some("red".into()));
        state.merge(original.clone());
        save(&path, &state).unwrap();
        let removed = state.issue(2, 11, "member/alice".into(), None);
        state.merge(removed.clone());
        save(&path, &state).unwrap();
        let mut restarted: Registers =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(restarted.entries["member/alice"], removed);
        assert!(!restarted.merge(original));
        let replacement = restarted.issue(2, 12, "member/alice".into(), Some("blue".into()));
        assert!(replacement.stamp > removed.stamp);
        assert!(restarted.merge(replacement));
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn forged_origin_or_stale_incarnation_rejects_the_entire_control_batch() {
        let mut issuer = Registers::default();
        let genuine = issuer.issue(2, 11, "enabled".into(), Some(true.into()));
        let mut forged = issuer.issue(3, 11, "whitelist/alice".into(), Some("Alice".into()));
        let mut replica = Registers::default();
        assert_eq!(
            replica.merge_authenticated(2, 11, vec![genuine.clone(), forged.clone()], |_| true),
            None
        );
        assert_eq!(replica.entries().count(), 0);
        forged.stamp.origin = 2;
        forged.stamp.incarnation = 10;
        assert_eq!(
            replica.merge_authenticated(2, 11, vec![genuine.clone(), forged], |_| true),
            None
        );
        assert_eq!(replica.entries().count(), 0);
        assert_eq!(
            replica.merge_authenticated(2, 11, vec![genuine], |_| true),
            Some(true)
        );
        assert_eq!(replica.entries().count(), 1);
    }

    #[test]
    fn contradictory_same_stamp_seed_and_batch_are_rejected_atomically() {
        let mut issuer = Registers::default();
        let original = issuer.issue(2, 11, "enabled".into(), Some(true.into()));
        let mut contradictory = original.clone();
        contradictory.value = Some(false.into());
        let mut replica = Registers::default();
        assert_eq!(
            replica.merge_authenticated(
                2,
                11,
                vec![original.clone(), contradictory.clone()],
                |_| true
            ),
            None
        );
        assert_eq!(replica.entries().count(), 0);
        replica.merge(original.clone());
        let unrelated = issuer.issue(3, 12, "other".into(), Some(true.into()));
        assert_eq!(
            replica.merge_reconciled(vec![unrelated, contradictory], |_| true),
            None
        );
        assert_eq!(replica.entries().count(), 1);
        assert_eq!(replica.entries["enabled"], original);
    }

    #[test]
    fn seed_capacity_returns_full_and_original_frame_can_resume_after_completion() {
        let mut assemblies = SeedAssemblies::default();
        for peer in 1..=64 {
            assert!(matches!(
                assemblies.push(peer, 1, [1; 16], 0, 2, vec![]),
                SeedResult::Pending
            ));
        }
        let mut issuer = Registers::default();
        let original = issuer.issue(65, 11, "member/alice".into(), Some("red".into()));
        assert!(!assemblies.can_accept(65, 1, [2; 16]));
        assert!(matches!(
            assemblies.push(65, 1, [2; 16], 0, 2, vec![original.clone()]),
            SeedResult::Full
        ));
        assert!(matches!(
            assemblies.push(1, 1, [1; 16], 1, 2, vec![]),
            SeedResult::Complete(_)
        ));
        assert!(assemblies.can_accept(65, 1, [2; 16]));
        assert!(matches!(
            assemblies.push(65, 1, [2; 16], 0, 2, vec![original.clone()]),
            SeedResult::Pending
        ));
        let SeedResult::Complete(entries) = assemblies.push(65, 1, [2; 16], 1, 2, vec![]) else {
            panic!("retained transfer did not resume")
        };
        assert_eq!(entries, [original]);
    }

    #[test]
    fn interleaved_same_peer_seed_transfers_complete_independently() {
        let mut state = Registers::default();
        let first = state.issue(2, 11, "first".into(), Some(true.into()));
        let second = state.issue(2, 11, "second".into(), Some(true.into()));
        let mut assemblies = SeedAssemblies::default();
        assert!(matches!(
            assemblies.push(2, 7, [1; 16], 0, 2, vec![first.clone()]),
            SeedResult::Pending
        ));
        assert!(matches!(
            assemblies.push(2, 7, [2; 16], 0, 2, vec![second.clone()]),
            SeedResult::Pending
        ));
        let SeedResult::Complete(a) = assemblies.push(2, 7, [1; 16], 1, 2, vec![]) else {
            panic!("first transfer lost")
        };
        let SeedResult::Complete(b) = assemblies.push(2, 7, [2; 16], 1, 2, vec![]) else {
            panic!("second transfer lost")
        };
        assert_eq!(a, [first.clone()]);
        assert_eq!(b, [second.clone()]);
        assert!(matches!(
            assemblies.push(2, 7, [3; 16], 0, 2, vec![first]),
            SeedResult::Pending
        ));
        assert!(matches!(
            assemblies.push(2, 7, [3; 16], 0, 2, vec![second]),
            SeedResult::Rejected("contradictory seed part")
        ));
    }

    #[test]
    fn reconnect_seed_carries_deleted_keys_and_rejects_other_domain_prefix() {
        let mut state = Registers::default();
        for index in 0..64 {
            let record = state.issue(
                2,
                11,
                format!("ip/{index}"),
                Some(serde_json::json!("x".repeat(16384))),
            );
            state.merge(record);
        }
        let deleted = state.issue(2, 11, "ip/7".into(), None);
        state.merge(deleted);
        let frames = seed_frames(b"PPolicy1", &state).unwrap();
        assert!(frames.len() > 1);
        let mut receiving = Registers::default();
        for bytes in frames.into_iter().rev() {
            assert!(decode(b"PScore01", &bytes).is_none());
            let Frame::Seed { entries, .. } = decode(b"PPolicy1", &bytes).unwrap() else {
                panic!();
            };
            for entry in entries {
                receiving.merge(entry);
            }
        }
        assert_eq!(state.entries, receiving.entries);
        assert!(receiving.entries["ip/7"].value.is_none());
    }
    #[test]
    fn cooldown_counts_suppressed_repeats_with_bounded_keys() {
        let mut diagnostics = Diagnostics::default();
        assert!(diagnostics.eligible(2, "invalid mutation"));
        assert!(!diagnostics.eligible(2, "invalid mutation"));
        assert!(!diagnostics.eligible(2, "invalid mutation"));
        assert_eq!(diagnostics.suppressed_count(), 2);
        for peer in 3..1026 {
            assert!(diagnostics.eligible(peer, "invalid mutation"));
        }
        assert!(!diagnostics.eligible(1026, "invalid mutation"));
        assert_eq!(diagnostics.last.len(), 1024);
        assert_eq!(diagnostics.suppressed_count(), 3);
    }
    #[test]
    fn unknown_publication_does_not_block_other_sources_and_original_frame_resumes() {
        let mut pending =
            std::collections::VecDeque::from([(2u16, 3u64, "original"), (4, 8, "known")]);
        let mut generations = BTreeMap::from([(2u16, 2u64), (4, 8)]);
        let mut boots = BTreeMap::from([(4u16, 90u64)]);
        let known = take_ready_control(&mut pending, |(peer, epoch, _)| {
            control_publication_ready(
                *epoch,
                generations.get(peer).copied(),
                boots.get(peer).copied(),
            )
        });
        assert_eq!(known, Some((4, 8, "known")));
        assert_eq!(pending.front(), Some(&(2, 3, "original")));
        generations.insert(2, 3);
        assert!(
            take_ready_control(&mut pending, |(peer, epoch, _)| control_publication_ready(
                *epoch,
                generations.get(peer).copied(),
                boots.get(peer).copied()
            ))
            .is_none()
        );
        boots.insert(2, 91);
        assert_eq!(
            take_ready_control(&mut pending, |(peer, epoch, _)| control_publication_ready(
                *epoch,
                generations.get(peer).copied(),
                boots.get(peer).copied()
            )),
            Some((2, 3, "original"))
        );
        assert!(pending.is_empty());
        assert!(control_publication_ready(2, Some(3), None));
        assert!(!control_publication_ready(3, None, None));
    }
}
