use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pumpkin_cluster::canonical_action::LivePlayerLease;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use sha2::{Digest, Sha256};

use super::{PendingParcel, Server, TestWire, encode, generation};
use crate::server::cluster_test_scenarios::{
    FixtureLoadoutTemplate, InventoryScenarioOutcome, InventoryScenarioPhase,
};

#[derive(Clone, Debug)]
pub(crate) struct FixtureLoadoutReceipt {
    pub token: [u8; 16],
    pub target: LivePlayerLease,
    pub template: FixtureLoadoutTemplate,
    pub grant_reference_digest: [u8; 32],
    pub grant_action_count: u16,
    pub restore_reference_digest: [u8; 32],
    grounded: (),
}

pub(super) struct RoutedLoadoutRequest {
    pub target: LivePlayerLease,
    pub template: FixtureLoadoutTemplate,
    pub token: [u8; 16],
    pub completion: tokio::sync::oneshot::Sender<Result<FixtureLoadoutReceipt, ()>>,
}

struct Progress {
    target: LivePlayerLease,
    template: FixtureLoadoutTemplate,
    host_generation: u64,
    return_to: Option<(ServerId, u64)>,
    captured: Option<([u8; 32], u16)>,
    accepted: bool,
    restored: bool,
    restore_digest: Option<[u8; 32]>,
    completion: Option<tokio::sync::oneshot::Sender<Result<FixtureLoadoutReceipt, ()>>>,
    callback_done: bool,
    failed: bool,
    created: Instant,
}

#[derive(Default)]
pub(super) struct Loadouts {
    progress: BTreeMap<[u8; 16], Progress>,
    failed_unclaimed: BTreeSet<[u8; 16]>,
}

impl Loadouts {
    pub(super) fn request(
        &mut self,
        server: &Arc<Server>,
        local: ServerId,
        target: LivePlayerLease,
        template: FixtureLoadoutTemplate,
        nonce: u64,
        pending: &mut std::collections::VecDeque<PendingParcel>,
    ) -> Option<[u8; 16]> {
        if self.progress.len() >= 64 {
            return None;
        }
        if lease_for(server, target.gid) != Some(target) {
            return None;
        }
        let gid = target.gid;
        let incarnation = target.incarnation;
        let host_generation = if gid.server == local {
            0
        } else {
            generation(server, gid.server)?
        };
        let mut hash = Sha256::new();
        hash.update(b"canonical fixture loadout");
        hash.update(nonce.to_le_bytes());
        hash.update(local.0.to_le_bytes());
        hash.update(host_generation.to_le_bytes());
        hash.update(target.key.0.as_bytes());
        hash.update(incarnation.to_le_bytes());
        let digest = hash.finalize();
        let token: [u8; 16] = digest[..16].try_into().ok()?;
        self.request_token(server, local, target, template, token, pending)
    }

    fn request_token(
        &mut self,
        server: &Arc<Server>,
        local: ServerId,
        target: LivePlayerLease,
        template: FixtureLoadoutTemplate,
        token: [u8; 16],
        pending: &mut std::collections::VecDeque<PendingParcel>,
    ) -> Option<[u8; 16]> {
        if self.progress.len() >= 64
            || token == [0; 16]
            || lease_for(server, target.gid) != Some(target)
        {
            return None;
        }
        let gid = target.gid;
        let host_generation = if gid.server == local {
            0
        } else {
            generation(server, gid.server)?
        };
        if self.progress.contains_key(&token) {
            return None;
        }
        if gid.server == local {
            if !submit_local(server, target, template, token) {
                return None;
            }
            self.progress.insert(
                token,
                Progress {
                    target,
                    template,
                    host_generation,
                    return_to: None,
                    captured: None,
                    accepted: false,
                    restored: false,
                    restore_digest: None,
                    completion: None,
                    callback_done: false,
                    failed: false,
                    created: Instant::now(),
                },
            );
            return Some(token);
        }
        let message = TestWire::LoadoutRequest {
            origin: local,
            target,
            template,
            token,
        };
        let bytes = encode(message)?;
        if pending.len() >= 128 {
            return None;
        }
        self.progress.insert(
            token,
            Progress {
                target,
                template,
                host_generation,
                return_to: None,
                captured: None,
                accepted: false,
                restored: false,
                restore_digest: None,
                completion: None,
                callback_done: false,
                failed: false,
                created: Instant::now(),
            },
        );
        pending.push_back(PendingParcel {
            target: gid.server,
            generation: host_generation,
            bytes,
        });
        Some(token)
    }

    pub(super) fn receive_request(
        &mut self,
        server: &Arc<Server>,
        local: ServerId,
        peer: ServerId,
        peer_generation: u64,
        origin: ServerId,
        target: LivePlayerLease,
        template: FixtureLoadoutTemplate,
        token: [u8; 16],
    ) {
        if origin != peer
            || target.gid.server != local
            || generation(server, peer) != Some(peer_generation)
            || self.progress.len() >= 64
            || self.progress.contains_key(&token)
        {
            return;
        }
        if !submit_local(server, target, template, token) {
            tracing::error!(?token, ?target, "cluster test canonical loadout rejected");
            return;
        }
        self.progress.insert(
            token,
            Progress {
                target,
                template,
                host_generation: generation(server, local).unwrap_or_default(),
                return_to: Some((peer, peer_generation)),
                captured: None,
                accepted: false,
                restored: false,
                restore_digest: None,
                completion: None,
                callback_done: false,
                failed: false,
                created: Instant::now(),
            },
        );
    }

    pub(super) fn receive_phase(
        &mut self,
        server: &Server,
        peer: ServerId,
        peer_generation: u64,
        token: [u8; 16],
        target: LivePlayerLease,
        template: FixtureLoadoutTemplate,
        phase: InventoryScenarioPhase,
        digest: [u8; 32],
        count: u16,
    ) {
        let Some(progress) = self.progress.get_mut(&token) else {
            return;
        };
        if progress.return_to.is_some()
            || peer != progress.target.gid.server
            || peer_generation != progress.host_generation
            || generation(server, peer) != Some(peer_generation)
            || target != progress.target
            || template != progress.template
        {
            return;
        }
        apply_phase(progress, phase, digest, count);
    }

    pub(super) fn advance(
        &mut self,
        server: &Arc<Server>,
        local: ServerId,
        pending: &mut std::collections::VecDeque<PendingParcel>,
    ) {
        for _ in 0..64 {
            let Some(request) = server.cluster_test.loadout_requests.pop() else {
                break;
            };
            if self
                .request_token(
                    server,
                    local,
                    request.target,
                    request.template,
                    request.token,
                    pending,
                )
                .is_some()
            {
                self.progress.get_mut(&request.token).unwrap().completion =
                    Some(request.completion);
            } else {
                let _ = request.completion.send(Err(()));
            }
        }
        for _ in 0..128 {
            let Some(outcome) = server.cluster_test.scenarios.pop_inventory_outcome() else {
                break;
            };
            if !self.progress.contains_key(&outcome.token) {
                if outcome.template != FixtureLoadoutTemplate::InitialAvatar {
                    let token = outcome.token;
                    if server
                        .cluster_test
                        .scenarios
                        .defer_inventory_outcome(outcome)
                        .is_err()
                        && self.failed_unclaimed.len() < 64
                        && self.failed_unclaimed.insert(token)
                    {
                        tracing::error!(
                            ?token,
                            "cluster test unclaimed inventory outcome capacity exhausted"
                        );
                    }
                    continue;
                }
                if outcome.template != FixtureLoadoutTemplate::InitialAvatar
                    || outcome.phase != InventoryScenarioPhase::Captured
                    || self.progress.len() >= 64
                {
                    tracing::error!(token=?outcome.token,"cluster test unregistered loadout outcome");
                    continue;
                }
                self.progress.insert(
                    outcome.token,
                    Progress {
                        target: outcome.target,
                        template: outcome.template,
                        host_generation: 0,
                        return_to: None,
                        captured: None,
                        accepted: false,
                        restored: false,
                        restore_digest: None,
                        completion: None,
                        callback_done: false,
                        failed: false,
                        created: Instant::now(),
                    },
                );
            }
            let progress = self.progress.get_mut(&outcome.token).unwrap();
            if progress.target != outcome.target || progress.template != outcome.template {
                progress.failed = true;
                continue;
            }
            let Some((digest, count)) = fingerprint(&outcome) else {
                progress.failed = true;
                continue;
            };
            apply_phase(progress, outcome.phase, digest, count);
            if progress.restored
                && !progress.failed
                && outcome.template == FixtureLoadoutTemplate::InitialAvatar
            {
                if let Some(player) = server.get_all_players().into_iter().find(|player| {
                    crate::server::cluster_world_owner::player_action_lease(server, player)
                        == Some(outcome.target)
                }) {
                    player.send_system_message(&pumpkin_util::text::TextComponent::text(
                        "cluster test initial-avatar-inventory accepted",
                    ));
                }
            }
            if let Some((target, epoch)) = progress.return_to {
                let message = TestWire::LoadoutPhase {
                    token: outcome.token,
                    target: outcome.target,
                    template: outcome.template,
                    phase: outcome.phase,
                    digest,
                    count,
                };
                if pending.len() >= 128 {
                    progress.failed = true;
                    continue;
                }
                if let Some(bytes) = encode(message) {
                    pending.push_back(PendingParcel {
                        target,
                        generation: epoch,
                        bytes,
                    });
                }
            }
        }
        for (token, progress) in &mut self.progress {
            if !progress.restored
                && !progress.failed
                && progress.created.elapsed() >= Duration::from_secs(40)
            {
                progress.failed = true;
                tracing::error!(?token, "cluster test canonical loadout outcome expired");
            }
            if (progress.failed || progress.restored)
                && let Some(completion) = progress.completion.take()
            {
                let receipt = if progress.failed {
                    Err(())
                } else {
                    progress
                        .captured
                        .zip(progress.restore_digest)
                        .map(
                            |(
                                (grant_reference_digest, grant_action_count),
                                restore_reference_digest,
                            )| FixtureLoadoutReceipt {
                                token: *token,
                                target: progress.target,
                                template: progress.template,
                                grant_reference_digest,
                                grant_action_count,
                                restore_reference_digest,
                                grounded: (),
                            },
                        )
                        .ok_or(())
                };
                let _ = completion.send(receipt);
                progress.callback_done = true;
            }
        }
        self.progress.retain(|_, progress| {
            !progress.callback_done
                && !(progress.restored
                    && (progress.return_to.is_some()
                        || progress.template == FixtureLoadoutTemplate::InitialAvatar))
        });
    }

    pub(super) fn ready(&self, token: Option<[u8; 16]>) -> bool {
        token
            .and_then(|token| self.progress.get(&token))
            .is_some_and(|progress| progress.restored && !progress.failed)
    }

    pub(super) fn retire(&mut self, token: Option<[u8; 16]>) {
        if let Some(token) = token {
            self.progress.remove(&token);
        }
    }
}

fn fingerprint(outcome: &InventoryScenarioOutcome) -> Option<([u8; 32], u16)> {
    if outcome.actions.is_empty() || outcome.actions.len() > 64 {
        return None;
    }
    let mut references = outcome.actions.clone();
    references.sort();
    references.dedup();
    if references.len() != outcome.actions.len() {
        return None;
    }
    let bytes = postcard::to_allocvec(&references).ok()?;
    Some((Sha256::digest(bytes).into(), references.len() as u16))
}

fn apply_phase(
    progress: &mut Progress,
    phase: InventoryScenarioPhase,
    digest: [u8; 32],
    count: u16,
) {
    if progress.failed || count == 0 || count > 64 {
        progress.failed = true;
        return;
    }
    match phase {
        InventoryScenarioPhase::Captured
            if progress.captured.is_none() && !progress.accepted && !progress.restored =>
        {
            progress.captured = Some((digest, count))
        }
        InventoryScenarioPhase::Accepted
            if progress.captured == Some((digest, count))
                && !progress.accepted
                && !progress.restored =>
        {
            progress.accepted = true
        }
        InventoryScenarioPhase::Restored
            if progress.accepted && !progress.restored && count == 1 =>
        {
            progress.restored = true;
            progress.restore_digest = Some(digest)
        }
        _ => progress.failed = true,
    }
}

pub(super) fn lease_for(server: &Server, gid: GlobalPlayerId) -> Option<LivePlayerLease> {
    let entry = crate::server::cluster_presence::all_presence_entries(server)
        .into_iter()
        .find(|(id, entry)| *id == gid && !entry.in_lobby)?
        .1;
    let incarnation = server
        .cluster_actor_statistics
        .load()
        .get(&gid)?
        .incarnation;
    Some(LivePlayerLease {
        key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::from_bytes(entry.uuid)),
        gid,
        incarnation,
    })
}

fn submit_local(
    server: &Arc<Server>,
    target: LivePlayerLease,
    template: FixtureLoadoutTemplate,
    token: [u8; 16],
) -> bool {
    let Some(player) = server.get_all_players().into_iter().find(|player| {
        crate::server::cluster_world_owner::player_action_lease(server, player) == Some(target)
    }) else {
        return false;
    };
    let source = crate::command::CommandSource::new(
        crate::command::CommandSender::Console,
        player.world(),
        None,
        Default::default(),
        Default::default(),
        "cluster-e2e-loadout".to_owned(),
        pumpkin_util::text::TextComponent::text("cluster-e2e-loadout"),
        Arc::clone(server),
    );
    crate::server::cluster_test_scenarios::submit_loadout(server, &source, target, template, token)
        .is_ok()
}
