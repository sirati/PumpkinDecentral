use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{Action, ActionId, ActionProposal, ActionRef, ApplyFailure, ClaimedAction, LivePlayerLease};
use pumpkin_cluster::entity_origin::EntityOriginAllocator;
use pumpkin_cluster::mob_breeding::MobBirthAdmission;
use pumpkin_cluster::mob_initialization::EntityAdmissionRequest;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

pub(super) struct FrozenMobAdmission {
    request: EntityAdmissionRequest,
    source: PeerGeneration,
    actor_chunk: ChunkKey,
    lease: Option<LivePlayerLease>,
    admission: Option<MobBirthAdmission>,
    reference: Option<ActionRef>,
    authority: Option<pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority>,
    source_slot: Option<u64>,
    issuance: Option<pumpkin_cluster::issuer_entity_admission::AcceptedCommandIssuance>,
}

impl FrozenMobAdmission {
    pub(super) fn capture(
        server: &super::Server,
        state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        proposal: &ActionProposal,
        source: PeerGeneration,
        executors: &std::collections::BTreeMap<ChunkKey, PeerGeneration>,
    ) -> Result<Self, ApplyFailure> {
        let ClaimedAction::EntityAdmissionRequest(request) = &proposal.claim
            else { return Err(ApplyFailure::InvalidAction); };
        if !request.valid() || source.generation == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let chunk = request.chunk().ok_or(ApplyFailure::InvalidAction)?;
        let kind = pumpkin_data::entity::EntityType::from_raw(request.kind)
            .filter(|kind| kind.mob || super::cluster_vehicle_birth::vehicle_kind(kind).is_some()).ok_or(ApplyFailure::InvalidAction)?;
        if if kind.mob { !super::cluster_owned_mob_metadata::species_matches_kind(&request.initial.species, kind.id) }
            else { !super::cluster_vehicle_birth::valid_initialization(&request.initial) } {
            return Err(ApplyFailure::InvalidAction);
        }
        let actor_chunk = match (&proposal.command, proposal.id.actor, proposal.lease) {
            (Some((origin, _transport_epoch, command)), actor, lease) => {
                use pumpkin_cluster::player_command_control::{CommandPrincipal, PlayerCommandMutation};
                if *origin != source || command.mutation != PlayerCommandMutation::Summon(request.clone()) {
                    return Err(ApplyFailure::InvalidAction);
                }

                match (&command.principal, actor, lease) {
                    (CommandPrincipal::Player { gid, uuid, .. }, ActorId::Player(actor), Some(lease))
                        if *gid == actor && actor == lease.gid && lease.key.0.as_bytes() == uuid
                            && lease.incarnation == proposal.id.incarnation
                            && state.inventory.live_player_lease_matches(lease) => {
                        state.actors.player_observations(state.living).find_map(|(gid, _, association, _)|
                            (gid == actor).then_some(association)).ok_or(ApplyFailure::InvalidAction)?
                    }
                    (CommandPrincipal::Console | CommandPrincipal::Player { .. }, ActorId::Peer(peer), None)
                        if peer == source.server && proposal.id.incarnation == source.generation
                            && matches!(command.target, pumpkin_cluster::player_command_control::CommandTarget::World { world, owner }
                                if world == chunk.world && owner == source) => chunk,
                    _ => return Err(ApplyFailure::InvalidAction),
                }
            }
            (None, ActorId::Peer(peer), None) if server.is_cluster_primary()
                && peer == source.server && proposal.id.incarnation == source.generation
                && executors.get(&chunk) == Some(&source) => chunk,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let issuance = proposal.command.as_ref().map(|(origin, transport_generation, command)|
            super::cluster_command_issuance::capture(server, *origin, *transport_generation, command, source)).transpose()?;
        Ok(Self { request: request.clone(), source, actor_chunk,
            lease: proposal.lease, admission: None, reference: None, authority: None, source_slot: None, issuance })
    }
    pub(super) fn chunk(&self) -> ChunkKey { self.request.chunk().unwrap() }
    pub(super) fn actor_chunk(&self) -> ChunkKey { self.actor_chunk }
    pub(super) fn lease(&self) -> Option<LivePlayerLease> { self.lease }
    pub(super) fn admission(&self) -> Option<&MobBirthAdmission> { self.admission.as_ref() }

    pub(super) fn bind(&mut self, id: ActionId, tick: TickStamp, allocator: &mut EntityOriginAllocator,
        snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot)
        -> Result<(), ApplyFailure> {
        let reference = ActionRef { id, tick };
        if self.reference.is_some_and(|original| original != reference) {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.admission.is_some() { return Ok(()); }
        if snapshot.original_tick() != tick { return Err(ApplyFailure::InvalidAction); }
        self.source_slot = Some(snapshot.original_source_slot().ok_or(ApplyFailure::MissingStakeholder(
            pumpkin_cluster::canonical_action::Stakeholder::Chunk(self.chunk())))?);
        if self.authority.is_none() && self.issuance.is_none() {
            let (rules, _) = snapshot.world_rules_at(self.chunk().world, tick)
                .ok_or(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(self.chunk())))?;
            self.authority = Some(pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority::Primary { rules });
        }
        let kind = pumpkin_data::entity::EntityType::from_raw(self.request.kind)
            .ok_or(ApplyFailure::InvalidAction)?;
        let (_, rules) = snapshot.world_rules_at(self.chunk().world, tick)
            .ok_or(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(self.chunk())))?;
        if rules.difficulty == 0 && !kind.category.is_friendly { return Err(ApplyFailure::InvalidAction); }
        let identities = allocator.reserve(self.source, reference, 1).map_err(|_| ApplyFailure::Capacity)?;
        let (body, supplied) = if kind.mob {
            super::cluster_owned_mob_initial::prepare_typed_body(&identities[0],
                reference, self.source, kind, self.request.spatial.clone(), &self.request.initial)?
        } else {
            super::cluster_vehicle_birth::prepare_reserved_body(&identities[0],
                reference, self.source, kind, self.request.spatial.clone(), &self.request.initial)?
        };
        let admission = super::cluster_owned_mob_birth::paired_admission(body, self.chunk(), 0, supplied)?;
        if let Some(issuance) = &self.issuance {
            self.authority = Some(pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority::Command {
                receipt: Box::new(issuance.bind(self.source_slot.ok_or(ApplyFailure::InvalidAction)?, &admission)?) });
        }
        self.admission = Some(admission);
        self.reference = Some(reference);
        Ok(())
    }

    pub(super) fn witness(&self) -> Result<pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness, ApplyFailure> {
        Ok(pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness {
            admission: self.admission.clone().ok_or(ApplyFailure::InvalidAction)?, request: self.request.clone(),
            issuer: self.source, actor_chunk: self.actor_chunk, lease: self.lease,
            source_slot: self.source_slot.ok_or(ApplyFailure::InvalidAction)?,
            authority: self.authority.clone().ok_or(ApplyFailure::InvalidAction)?,
        })
    }

    pub(super) fn materialize(&self) -> Result<Action, ApplyFailure> {
        self.witness()?.capture_action(pumpkin_cluster::canonical_action::ClientOrigin::None)
    }
}

pub(super) fn validate_initialization(witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness)
    -> Result<(), ApplyFailure> {
    if !witness.valid() { return Err(ApplyFailure::InvalidAction); }
    let expected = &witness.admission;
    let identity = pumpkin_cluster::entity_origin::EntityOriginIdentity {
        id: expected.body.id, uuid: expected.body.birth.uuid, source: witness.issuer };
    let kind = pumpkin_data::entity::EntityType::from_raw(witness.request.kind).ok_or(ApplyFailure::InvalidAction)?;
    let (body, supplied) = if kind.mob {
        super::cluster_owned_mob_initial::prepare_typed_body(&identity, expected.body.birth.action,
            witness.issuer, kind, witness.request.spatial.clone(), &witness.request.initial)?
    } else {
        super::cluster_vehicle_birth::prepare_reserved_body(&identity, expected.body.birth.action,
            witness.issuer, kind, witness.request.spatial.clone(), &witness.request.initial)?
    };
    if super::cluster_owned_mob_birth::paired_admission(body, expected.chunk, 0, supplied)? != *expected {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(())
}

pub(crate) fn accepted_birth(
    server: &super::Server,
    reference: ActionRef,
    request: &EntityAdmissionRequest,
) -> Result<Option<(pumpkin_cluster::item_contact::ContactIdentity, std::sync::Arc<dyn crate::entity::EntityBase>)>, ApplyFailure> {
    let chunk = request.chunk().ok_or(ApplyFailure::InvalidAction)?;
    let directory = server.cluster_canonical_entities.load();
    let mut result = None;
    for entity in directory.birth_entities(reference) {
        let Some(shell) = entity.cast_any().downcast_ref::<super::cluster_entity_registry::CanonicalEntityShell>() else { continue; };
        if shell.body.birth.action != reference { continue; }
        if shell.body.kind != request.kind || shell.body.spatial.world != chunk.world || result.is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        result = Some((pumpkin_cluster::item_contact::ContactIdentity::Entity {
            id: shell.body.id, birth: shell.body.birth.clone() }, entity.clone()));
    }
    Ok(result)
}

pub(crate) fn command_receipt(action: &Action)
    -> Option<&pumpkin_cluster::issuer_entity_admission::CommandAdmissionReceipt> {
    let witness = pumpkin_cluster::issuer_entity_admission::witness(action)?;
    let pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority::Command { receipt } = &witness.authority else { return None; };
    (witness.guards_valid(action) && receipt.matches(witness)).then_some(receipt.as_ref())
}
