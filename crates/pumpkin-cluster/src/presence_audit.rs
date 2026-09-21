use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::identity::GlobalPlayerId;
use crate::presence::{PresenceLogin, PresenceLogout, PresenceTable, is_valid_presence_name};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisiblePlayer {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
}

impl VisiblePlayer {
    #[must_use]
    pub fn new(gid: GlobalPlayerId, uuid: [u8; 16], name: String) -> Self {
        Self { gid, uuid, name }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SurfaceKind {
    Online,
    Tab,
    Motd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceSurfaces {
    pub count: usize,
    pub online: Vec<VisiblePlayer>,
    pub tab: Vec<VisiblePlayer>,
    pub motd_sample: Vec<VisiblePlayer>,
}

impl PresenceSurfaces {
    #[must_use]
    pub fn new(
        count: usize,
        online: Vec<VisiblePlayer>,
        tab: Vec<VisiblePlayer>,
        motd_sample: Vec<VisiblePlayer>,
    ) -> Self {
        Self {
            count,
            online,
            tab,
            motd_sample,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditIssue {
    CountMismatch { expected: usize, advertised: usize },
    OnlineGhost { gid: GlobalPlayerId },
    OnlineMissing { gid: GlobalPlayerId },
    OnlineOrder,
    TabGhost { gid: GlobalPlayerId },
    TabMissing { gid: GlobalPlayerId },
    TabOrder,
    MotdGhost { gid: GlobalPlayerId },
    MotdMissing { gid: GlobalPlayerId },
    MotdOrder,
    MotdOverLimit { len: usize, limit: usize },
    HiddenLeaked { gid: GlobalPlayerId, surface: SurfaceKind },
    DuplicateAdvertised { gid: GlobalPlayerId, surface: SurfaceKind },
    GhostEntry { gid: GlobalPlayerId },
    MissingEntry { gid: GlobalPlayerId },
    UuidMismatch { gid: GlobalPlayerId },
    NameMismatch { gid: GlobalPlayerId },
    InvalidName { gid: GlobalPlayerId },
    DuplicateUuid { uuid: [u8; 16] },
    DuplicateName { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuditReport {
    pub issues: Vec<AuditIssue>,
}

impl AuditReport {
    #[must_use]
    pub fn new() -> Self {
        Self { issues: Vec::new() }
    }

    pub fn push(&mut self, issue: AuditIssue) {
        self.issues.push(issue);
    }

    #[must_use]
    pub const fn is_consistent(&self) -> bool {
        self.issues.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.issues.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }

    #[must_use]
    pub fn has_hidden_leak(&self) -> bool {
        self.issues
            .iter()
            .any(|issue| matches!(issue, AuditIssue::HiddenLeaked { .. }))
    }
}

#[must_use]
pub fn is_hidden(
    gid: &GlobalPlayerId,
    uuid: &[u8; 16],
    hidden_gids: &HashSet<GlobalPlayerId>,
    hidden_uuids: &HashSet<[u8; 16]>,
) -> bool {
    hidden_gids.contains(gid) || hidden_uuids.contains(uuid)
}

#[must_use]
pub fn visible_roster(
    table: &PresenceTable,
    hidden_gids: &HashSet<GlobalPlayerId>,
    hidden_uuids: &HashSet<[u8; 16]>,
) -> Vec<VisiblePlayer> {
    let mut roster: Vec<VisiblePlayer> = table
        .entries_iter()
        .filter(|(gid, entry)| !is_hidden(gid, &entry.uuid, hidden_gids, hidden_uuids))
        .map(|(gid, entry)| VisiblePlayer::new(*gid, entry.uuid, entry.name.clone()))
        .collect();
    roster.sort_by(|left, right| left.gid.cmp(&right.gid));
    roster
}

#[must_use]
pub fn motd_capped(roster: &[VisiblePlayer], limit: usize) -> Vec<VisiblePlayer> {
    roster.iter().take(limit).cloned().collect()
}

#[must_use]
pub fn surfaces_for_roster(roster: &[VisiblePlayer], motd_limit: usize) -> PresenceSurfaces {
    PresenceSurfaces::new(
        roster.len(),
        roster.to_vec(),
        roster.to_vec(),
        motd_capped(roster, motd_limit),
    )
}

#[must_use]
pub fn surfaces_from_table(
    table: &PresenceTable,
    hidden_gids: &HashSet<GlobalPlayerId>,
    hidden_uuids: &HashSet<[u8; 16]>,
    motd_limit: usize,
) -> PresenceSurfaces {
    surfaces_for_roster(
        &visible_roster(table, hidden_gids, hidden_uuids),
        motd_limit,
    )
}

#[must_use]
pub const fn logout_for_hidden(gid: GlobalPlayerId) -> PresenceLogout {
    PresenceLogout::new(gid)
}

#[must_use]
pub fn login_for_unhidden(entry: &VisiblePlayer) -> PresenceLogin {
    PresenceLogin::new(
        entry.gid,
        entry.uuid,
        entry.name.clone(),
        Vec::new(),
        crate::protocol::PlayerGameMode::Survival,
        false,
    )
}

fn ordered_gids(view: &[VisiblePlayer]) -> Vec<GlobalPlayerId> {
    view.iter().map(|entry| entry.gid).collect()
}

fn sorted_gids(view: &[VisiblePlayer]) -> Vec<GlobalPlayerId> {
    let mut gids = ordered_gids(view);
    gids.sort_unstable();
    gids
}

fn audit_surface_list(
    report: &mut AuditReport,
    expected: &[VisiblePlayer],
    advertised: &[VisiblePlayer],
    surface: SurfaceKind,
    motd_limit: Option<usize>,
) {
    let expected_gids = sorted_gids(expected);
    let advertised_gids = sorted_gids(advertised);
    let expected_set: HashSet<GlobalPlayerId> = expected_gids.iter().copied().collect();
    let advertised_set: HashSet<GlobalPlayerId> = advertised_gids.iter().copied().collect();
    for gid in &expected_gids {
        if !advertised_set.contains(gid) {
            match surface {
                SurfaceKind::Online => report.push(AuditIssue::OnlineMissing { gid: *gid }),
                SurfaceKind::Tab => report.push(AuditIssue::TabMissing { gid: *gid }),
                SurfaceKind::Motd => report.push(AuditIssue::MotdMissing { gid: *gid }),
            }
        }
    }
    for gid in &advertised_gids {
        if !expected_set.contains(gid) {
            match surface {
                SurfaceKind::Online => report.push(AuditIssue::OnlineGhost { gid: *gid }),
                SurfaceKind::Tab => report.push(AuditIssue::TabGhost { gid: *gid }),
                SurfaceKind::Motd => report.push(AuditIssue::MotdGhost { gid: *gid }),
            }
        }
    }
    let mut seen: HashSet<GlobalPlayerId> = HashSet::new();
    for gid in advertised_gids.iter() {
        if !seen.insert(*gid) {
            report.push(AuditIssue::DuplicateAdvertised {
                gid: *gid,
                surface,
            });
        }
    }
    if ordered_gids(advertised) != advertised_gids {
        match surface {
            SurfaceKind::Online => report.push(AuditIssue::OnlineOrder),
            SurfaceKind::Tab => report.push(AuditIssue::TabOrder),
            SurfaceKind::Motd => report.push(AuditIssue::MotdOrder),
        }
    }
    if let Some(limit) = motd_limit {
        if advertised.len() > limit {
            report.push(AuditIssue::MotdOverLimit {
                len: advertised.len(),
                limit,
            });
        }
    }
}

#[must_use]
pub fn audit_surfaces(
    expected: &[VisiblePlayer],
    advertised: &PresenceSurfaces,
    hidden_gids: &HashSet<GlobalPlayerId>,
    hidden_uuids: &HashSet<[u8; 16]>,
    motd_limit: usize,
) -> AuditReport {
    let mut report = AuditReport::new();
    if advertised.count != expected.len() {
        report.push(AuditIssue::CountMismatch {
            expected: expected.len(),
            advertised: advertised.count,
        });
    }
    audit_surface_list(
        &mut report,
        expected,
        &advertised.online,
        SurfaceKind::Online,
        None,
    );
    audit_surface_list(&mut report, expected, &advertised.tab, SurfaceKind::Tab, None);
    let expected_motd = motd_capped(expected, motd_limit);
    audit_surface_list(
        &mut report,
        &expected_motd,
        &advertised.motd_sample,
        SurfaceKind::Motd,
        Some(motd_limit),
    );
    for entry in &advertised.online {
        if is_hidden(&entry.gid, &entry.uuid, hidden_gids, hidden_uuids) {
            report.push(AuditIssue::HiddenLeaked {
                gid: entry.gid,
                surface: SurfaceKind::Online,
            });
        }
    }
    for entry in &advertised.tab {
        if is_hidden(&entry.gid, &entry.uuid, hidden_gids, hidden_uuids) {
            report.push(AuditIssue::HiddenLeaked {
                gid: entry.gid,
                surface: SurfaceKind::Tab,
            });
        }
    }
    for entry in &advertised.motd_sample {
        if is_hidden(&entry.gid, &entry.uuid, hidden_gids, hidden_uuids) {
            report.push(AuditIssue::HiddenLeaked {
                gid: entry.gid,
                surface: SurfaceKind::Motd,
            });
        }
    }
    report
}

#[must_use]
pub fn audit_peer_rosters(
    local: &[VisiblePlayer],
    peer: &[VisiblePlayer],
    hidden_gids: &HashSet<GlobalPlayerId>,
    hidden_uuids: &HashSet<[u8; 16]>,
) -> AuditReport {
    let mut report = AuditReport::new();
    let local_by_gid: HashMap<GlobalPlayerId, &VisiblePlayer> =
        local.iter().map(|entry| (entry.gid, entry)).collect();
    let peer_by_gid: HashMap<GlobalPlayerId, &VisiblePlayer> =
        peer.iter().map(|entry| (entry.gid, entry)).collect();
    for entry in local {
        match peer_by_gid.get(&entry.gid) {
            None => report.push(AuditIssue::MissingEntry { gid: entry.gid }),
            Some(peer_entry) => {
                if peer_entry.uuid != entry.uuid {
                    report.push(AuditIssue::UuidMismatch { gid: entry.gid });
                }
                if peer_entry.name != entry.name {
                    report.push(AuditIssue::NameMismatch { gid: entry.gid });
                }
            }
        }
    }
    for entry in peer {
        if !local_by_gid.contains_key(&entry.gid) {
            report.push(AuditIssue::GhostEntry { gid: entry.gid });
        }
        if is_hidden(&entry.gid, &entry.uuid, hidden_gids, hidden_uuids) {
            report.push(AuditIssue::HiddenLeaked {
                gid: entry.gid,
                surface: SurfaceKind::Online,
            });
        }
    }
    report
}

#[must_use]
pub fn audit_roster_integrity(roster: &[VisiblePlayer]) -> AuditReport {
    let mut report = AuditReport::new();
    let mut seen_gids: HashSet<GlobalPlayerId> = HashSet::new();
    let mut seen_uuids: HashSet<[u8; 16]> = HashSet::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    for entry in roster {
        if !seen_gids.insert(entry.gid) {
            report.push(AuditIssue::GhostEntry { gid: entry.gid });
        }
        if !seen_uuids.insert(entry.uuid) {
            report.push(AuditIssue::DuplicateUuid { uuid: entry.uuid });
        }
        if !is_valid_presence_name(&entry.name) {
            report.push(AuditIssue::InvalidName { gid: entry.gid });
        }
        if !seen_names.insert(entry.name.to_lowercase()) {
            report.push(AuditIssue::DuplicateName {
                name: entry.name.clone(),
            });
        }
    }
    if ordered_gids(roster) != sorted_gids(roster) {
        report.push(AuditIssue::OnlineOrder);
    }
    report
}

#[must_use]
pub fn audit_hide_transition(
    before: &[VisiblePlayer],
    after: &[VisiblePlayer],
    gid: &GlobalPlayerId,
    hidden: bool,
) -> AuditReport {
    let mut report = AuditReport::new();
    let before_has = before.iter().any(|entry| entry.gid == *gid);
    let after_has = after.iter().any(|entry| entry.gid == *gid);
    if hidden {
        if before_has && after_has {
            report.push(AuditIssue::HiddenLeaked {
                gid: *gid,
                surface: SurfaceKind::Online,
            });
        }
        if !before_has && !after_has {
            report.push(AuditIssue::MissingEntry { gid: *gid });
        }
    } else if before_has && !after_has {
        report.push(AuditIssue::MissingEntry { gid: *gid });
    } else if !before_has && !after_has {
        report.push(AuditIssue::MissingEntry { gid: *gid });
    }
    for entry in after {
        if entry.gid != *gid && !before.iter().any(|before_entry| before_entry.gid == entry.gid) {
            report.push(AuditIssue::GhostEntry { gid: entry.gid });
        }
    }
    for entry in before {
        if entry.gid != *gid && !after.iter().any(|after_entry| after_entry.gid == entry.gid) {
            report.push(AuditIssue::MissingEntry { gid: entry.gid });
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};
    use crate::presence::PresenceLogin;

    const MOTD_LIMIT: usize = 12;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn entry(server: u16, player: u16, name: &str) -> VisiblePlayer {
        VisiblePlayer::new(gid(server, player), [player as u8; 16], name.to_string())
    }

    fn table_with(names: &[(&str, u16, u16)]) -> PresenceTable {
        let mut table = PresenceTable::new();
        for (name, server, player) in names {
            let login = PresenceLogin::new(
                gid(*server, *player),
                [*player as u8; 16],
                (*name).to_string(),
                Vec::new(),
                crate::protocol::PlayerGameMode::Survival,
                false,
            );
            assert!(table.apply_login(&login, 100));
        }
        table
    }

    fn empty_hide() -> (HashSet<GlobalPlayerId>, HashSet<[u8; 16]>) {
        (HashSet::new(), HashSet::new())
    }

    #[test]
    fn roster_sorts_by_gid_and_counts() {
        let table = table_with(&[("Zed", 2, 9), ("Amy", 1, 3), ("Bo", 1, 7)]);
        let (hidden_gids, hidden_uuids) = empty_hide();
        let roster = visible_roster(&table, &hidden_gids, &hidden_uuids);
        assert_eq!(ordered_gids(&roster), vec![gid(1, 3), gid(1, 7), gid(2, 9)]);
        let surfaces = surfaces_for_roster(&roster, MOTD_LIMIT);
        assert_eq!(surfaces.count, 3);
        assert_eq!(surfaces.online, roster);
        assert_eq!(surfaces.tab, roster);
        assert_eq!(surfaces.motd_sample, roster);
        let report = audit_surfaces(&roster, &surfaces, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.is_consistent());
        assert!(report.is_empty());
        assert_eq!(report.len(), 0);
        assert!(!report.has_hidden_leak());
    }

    #[test]
    fn hidden_gid_and_uuid_are_removed_everywhere() {
        let table = table_with(&[("Amy", 1, 3), ("Bo", 1, 7), ("Zed", 2, 9)]);
        let mut hidden_gids = HashSet::new();
        hidden_gids.insert(gid(1, 7));
        let mut hidden_uuids = HashSet::new();
        hidden_uuids.insert([9_u8; 16]);
        let roster = visible_roster(&table, &hidden_gids, &hidden_uuids);
        assert_eq!(roster, vec![entry(1, 3, "Amy")]);
        let surfaces = surfaces_from_table(&table, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert_eq!(surfaces.count, 1);
        let report = audit_surfaces(&roster, &surfaces, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.is_consistent());
        assert!(is_hidden(&gid(1, 7), &[7_u8; 16], &hidden_gids, &hidden_uuids));
        assert!(is_hidden(&gid(2, 9), &[9_u8; 16], &hidden_gids, &hidden_uuids));
        assert!(!is_hidden(&gid(1, 3), &[3_u8; 16], &hidden_gids, &hidden_uuids));
    }

    #[test]
    fn count_and_tab_divergence_are_flagged() {
        let roster = vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")];
        let (hidden_gids, hidden_uuids) = empty_hide();
        let mut surfaces = surfaces_for_roster(&roster, MOTD_LIMIT);
        surfaces.count = 5;
        surfaces.tab.pop();
        let report = audit_surfaces(&roster, &surfaces, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(!report.is_consistent());
        assert!(report.issues.contains(&AuditIssue::CountMismatch {
            expected: 2,
            advertised: 5
        }));
        assert!(report.issues.contains(&AuditIssue::TabMissing { gid: gid(1, 7) }));
    }

    #[test]
    fn ghost_online_and_motd_leak_are_flagged() {
        let roster = vec![entry(1, 3, "Amy")];
        let (hidden_gids, hidden_uuids) = empty_hide();
        let advertised = PresenceSurfaces::new(
            2,
            vec![entry(1, 3, "Amy"), entry(9, 9, "Ghost")],
            vec![entry(1, 3, "Amy")],
            vec![entry(1, 3, "Amy"), entry(9, 9, "Ghost")],
        );
        let report = audit_surfaces(&roster, &advertised, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.issues.contains(&AuditIssue::OnlineGhost { gid: gid(9, 9) }));
        assert!(report.issues.contains(&AuditIssue::MotdGhost { gid: gid(9, 9) }));
        assert!(!report.has_hidden_leak());
    }

    #[test]
    fn hidden_player_in_surfaces_is_a_leak() {
        let roster = vec![entry(1, 3, "Amy")];
        let mut hidden_gids = HashSet::new();
        hidden_gids.insert(gid(1, 7));
        let (_, hidden_uuids) = empty_hide();
        let advertised = PresenceSurfaces::new(
            2,
            vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")],
            vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")],
            vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")],
        );
        let report = audit_surfaces(&roster, &advertised, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            AuditIssue::HiddenLeaked { gid: leaked, .. } if *leaked == gid(1, 7)
        )));
        assert!(report.has_hidden_leak());
    }

    #[test]
    fn unordered_and_duplicate_surfaces_fail() {
        let roster = vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")];
        let (hidden_gids, hidden_uuids) = empty_hide();
        let advertised = PresenceSurfaces::new(
            2,
            vec![entry(1, 7, "Bo"), entry(1, 3, "Amy")],
            vec![entry(1, 3, "Amy"), entry(1, 3, "Amy")],
            motd_capped(&roster, MOTD_LIMIT),
        );
        let report = audit_surfaces(&roster, &advertised, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.issues.contains(&AuditIssue::OnlineOrder));
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            AuditIssue::DuplicateAdvertised { gid: leaked, surface: SurfaceKind::Tab }
            if *leaked == gid(1, 3)
        )));
    }

    #[test]
    fn motd_sample_is_capped_prefix() {
        let roster: Vec<VisiblePlayer> = (0..20)
            .map(|player| entry(1, player, &format!("P{player:02}")))
            .collect();
        let capped = motd_capped(&roster, MOTD_LIMIT);
        assert_eq!(capped.len(), MOTD_LIMIT);
        assert_eq!(capped, roster[..MOTD_LIMIT].to_vec());
        let surfaces = surfaces_for_roster(&roster, MOTD_LIMIT);
        assert_eq!(surfaces.count, 20);
        assert_eq!(surfaces.online.len(), 20);
        assert_eq!(surfaces.motd_sample.len(), MOTD_LIMIT);
        let (hidden_gids, hidden_uuids) = empty_hide();
        let report = audit_surfaces(&roster, &surfaces, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.is_consistent());
        let mut overfull = surfaces;
        overfull.motd_sample = roster.clone();
        let report = audit_surfaces(&roster, &overfull, &hidden_gids, &hidden_uuids, MOTD_LIMIT);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            AuditIssue::MotdOverLimit { .. }
        )));
    }

    #[test]
    fn peer_rosters_diff_field_by_field() {
        let local = vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")];
        let mut peer = vec![entry(1, 3, "Amy"), entry(2, 1, "New")];
        peer[0].name = String::from("AmyX");
        let (hidden_gids, hidden_uuids) = empty_hide();
        let report = audit_peer_rosters(&local, &peer, &hidden_gids, &hidden_uuids);
        assert!(report.issues.contains(&AuditIssue::MissingEntry { gid: gid(1, 7) }));
        assert!(report.issues.contains(&AuditIssue::GhostEntry { gid: gid(2, 1) }));
        assert!(report.issues.contains(&AuditIssue::NameMismatch { gid: gid(1, 3) }));
        let mut peer_uuid = entry(1, 3, "Amy");
        peer_uuid.uuid = [99_u8; 16];
        let report = audit_peer_rosters(&local, &[peer_uuid], &hidden_gids, &hidden_uuids);
        assert!(report.issues.contains(&AuditIssue::UuidMismatch { gid: gid(1, 3) }));
        let clean = audit_peer_rosters(&local, &local, &hidden_gids, &hidden_uuids);
        assert!(clean.is_consistent());
    }

    #[test]
    fn roster_integrity_catches_collisions() {
        let roster = vec![entry(1, 3, "Amy"), entry(1, 7, "amy")];
        let report = audit_roster_integrity(&roster);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            AuditIssue::DuplicateName { .. }
        )));
        let mut dup_uuid = entry(2, 5, "Zed");
        dup_uuid.uuid = [3_u8; 16];
        let report = audit_roster_integrity(&[entry(1, 3, "Amy"), dup_uuid]);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            AuditIssue::DuplicateUuid { .. }
        )));
        let bad = vec![VisiblePlayer::new(gid(1, 3), [3_u8; 16], String::new())];
        let report = audit_roster_integrity(&bad);
        assert!(report.issues.contains(&AuditIssue::InvalidName { gid: gid(1, 3) }));
        let unordered = vec![entry(1, 7, "Bo"), entry(1, 3, "Amy")];
        let report = audit_roster_integrity(&unordered);
        assert!(report.issues.contains(&AuditIssue::OnlineOrder));
    }

    #[test]
    fn hide_transition_matches_logout_and_unhide_matches_login() {
        let before = vec![entry(1, 3, "Amy"), entry(1, 7, "Bo")];
        let after = vec![entry(1, 3, "Amy")];
        let report = audit_hide_transition(&before, &after, &gid(1, 7), true);
        assert!(report.is_consistent());
        let logout = logout_for_hidden(gid(1, 7));
        assert_eq!(logout.gid, gid(1, 7));
        let login = login_for_unhidden(&entry(1, 7, "Bo"));
        assert_eq!(login.gid, gid(1, 7));
        assert_eq!(login.uuid, [7_u8; 16]);
        assert_eq!(login.name, String::from("Bo"));
        let leaked = audit_hide_transition(&before, &before, &gid(1, 7), true);
        assert!(leaked.issues.iter().any(|issue| matches!(
            issue,
            AuditIssue::HiddenLeaked { .. }
        )));
        let restored = audit_hide_transition(&after, &before, &gid(1, 7), false);
        assert!(restored.is_consistent());
        let still_gone = audit_hide_transition(&before, &after, &gid(1, 7), false);
        assert!(still_gone.issues.contains(&AuditIssue::MissingEntry { gid: gid(1, 7) }));
    }
}
