use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionKind, ActionRef, ActionState, ApplyFailure, ClientOrigin, Condition,
    Effect, Expected, LivePlayerLease, SpatialEpoch, Stakeholder, Target, UndoProcedure,
};
use crate::mob_breeding::MobBirthAdmission;
use crate::mob_initialization::EntityAdmissionRequest;
use crate::player_command_control::CommandPrincipal;
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::world_rules::WorldRulesWitness;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPermissionSnapshot {
    pub revision: u64,
    pub level: u8,
}

impl CommandPermissionSnapshot {
    pub fn valid_for(self, principal: &CommandPrincipal) -> bool {
        self.level <= 4
            && match principal {
                CommandPrincipal::Console => self.level == 4 && self.revision == 0,
                CommandPrincipal::Player {
                    permission_revision,
                    ..
                } => self.revision == *permission_revision,
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedCommandIssuance {
    pub issuer: PeerGeneration,
    pub origin: PeerGeneration,
    pub transport_generation: u64,
    pub principal: CommandPrincipal,
    pub correlation: [u8; 16],
    pub permission_snapshot: CommandPermissionSnapshot,
    pub request_digest: [u8; 32],
}

impl AcceptedCommandIssuance {
    pub fn valid(&self) -> bool {
        self.issuer.generation != 0
            && self.origin.generation != 0
            && self.correlation != [0; 16]
            && self.permission_snapshot.valid_for(&self.principal)
            && match &self.principal {
                CommandPrincipal::Player { gid, uuid, .. } => {
                    gid.server == self.origin.server && *uuid != [0; 16]
                }
                CommandPrincipal::Console => true,
            }
    }

    pub fn bind(
        &self,
        source_slot: u64,
        admission: &MobBirthAdmission,
    ) -> Result<CommandAdmissionReceipt, ApplyFailure> {
        let reference = admission.body.birth.action;
        if !self.valid()
            || source_slot as u16 != reference.tick.0
            || admission.body.birth.source != self.issuer
            || !admission.inventory.valid()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut receipt = CommandAdmissionReceipt {
            issuance: self.clone(),
            reference,
            source_slot,
            admission_digest: admission_digest(admission)?,
            binding_digest: [0; 32],
        };
        receipt.binding_digest = receipt.compute_binding_digest()?;
        Ok(receipt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandAdmissionReceipt {
    pub issuance: AcceptedCommandIssuance,
    pub reference: ActionRef,
    pub source_slot: u64,
    pub admission_digest: [u8; 32],
    pub binding_digest: [u8; 32],
}

fn admission_digest(admission: &MobBirthAdmission) -> Result<[u8; 32], ApplyFailure> {
    let bytes = postcard::to_allocvec(admission).map_err(|_| ApplyFailure::InvalidAction)?;
    Ok(Sha256::digest(bytes).into())
}

impl CommandAdmissionReceipt {
    fn compute_binding_digest(&self) -> Result<[u8; 32], ApplyFailure> {
        let bytes = postcard::to_allocvec(&(
            &self.issuance,
            self.reference,
            self.source_slot,
            self.admission_digest,
        ))
        .map_err(|_| ApplyFailure::InvalidAction)?;
        Ok(Sha256::digest(bytes).into())
    }

    pub fn valid_binding(&self) -> bool {
        self.issuance.valid()
            && self.source_slot as u16 == self.reference.tick.0
            && self
                .compute_binding_digest()
                .is_ok_and(|digest| digest == self.binding_digest)
    }

    pub fn matches(&self, witness: &IssuerAdmissionWitness) -> bool {
        self.compute_binding_digest()
            .is_ok_and(|digest| digest == self.binding_digest)
            && self.issuance.valid()
            && self.issuance.issuer == witness.issuer
            && self.reference == witness.admission.body.birth.action
            && self.source_slot == witness.source_slot
            && self.source_slot as u16 == self.reference.tick.0
            && request_digest(&witness.request)
                .is_ok_and(|digest| digest == self.issuance.request_digest)
            && admission_digest(&witness.admission)
                .is_ok_and(|digest| digest == self.admission_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssuerAdmissionAuthority {
    Primary {
        rules: WorldRulesWitness,
    },
    Command {
        receipt: Box<CommandAdmissionReceipt>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerAdmissionWitness {
    pub admission: MobBirthAdmission,
    pub request: EntityAdmissionRequest,
    pub issuer: PeerGeneration,
    pub actor_chunk: ChunkKey,
    pub lease: Option<LivePlayerLease>,
    pub source_slot: u64,
    pub authority: IssuerAdmissionAuthority,
}

pub fn request_digest(request: &EntityAdmissionRequest) -> Result<[u8; 32], ApplyFailure> {
    if !request.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let bytes = postcard::to_allocvec(request).map_err(|_| ApplyFailure::InvalidAction)?;
    Ok(Sha256::digest(bytes).into())
}

impl IssuerAdmissionWitness {
    pub fn valid(&self) -> bool {
        let body = &self.admission.body;
        let reference = body.birth.action;
        self.request.valid()
            && self.request.chunk() == Some(self.admission.chunk)
            && self.request.kind == body.kind
            && self.request.spatial == body.spatial
            && self.issuer.generation != 0
            && body.birth.source == self.issuer
            && body.owner == self.issuer
            && body.id.origin == self.issuer.server
            && body.id.generation == self.issuer.generation
            && !body.birth.uuid.is_nil()
            && body.belongs_to(body.id, self.admission.chunk)
            && body.valid_at(reference.tick)
            && body.spatial_epoch == SpatialEpoch::Action(reference)
            && self.admission.inventory.imported_revision == 0
            && self.admission.inventory.valid()
            && self.source_slot as u16 == reference.tick.0
            && match (&self.authority, reference.id.actor, self.lease) {
                (IssuerAdmissionAuthority::Primary { rules }, ActorId::Peer(peer), None) => {
                    peer == self.issuer.server
                        && reference.id.incarnation == self.issuer.generation
                        && self.actor_chunk == self.admission.chunk
                        && rules.rules.issuer.generation != 0
                        && rules.rules.epoch != 0
                        && rules.rules.world == self.admission.chunk.world
                        && rules.slot == self.source_slot
                }
                (IssuerAdmissionAuthority::Command { receipt }, actor, lease) => {
                    let principal = &receipt.issuance.principal;
                    receipt.matches(self)
                        && match (principal, actor, lease) {
                            (
                                CommandPrincipal::Player { gid, uuid, .. },
                                ActorId::Player(actor),
                                Some(lease),
                            ) => {
                                *gid == actor
                                    && actor == lease.gid
                                    && gid.server == self.issuer.server
                                    && lease.incarnation != 0
                                    && !lease.key.0.is_nil()
                                    && lease.key.0.as_bytes() == uuid
                                    && lease.incarnation == reference.id.incarnation
                            }
                            (
                                CommandPrincipal::Console | CommandPrincipal::Player { .. },
                                ActorId::Peer(peer),
                                None,
                            ) => {
                                peer == self.issuer.server
                                    && reference.id.incarnation == self.issuer.generation
                                    && self.actor_chunk == self.admission.chunk
                                    && match principal {
                                        CommandPrincipal::Player { gid, uuid, .. } => {
                                            gid.server == self.issuer.server && *uuid != [0; 16]
                                        }
                                        CommandPrincipal::Console => true,
                                    }
                            }
                            _ => false,
                        }
                }
                _ => false,
            }
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        if !self.valid()
            || action.id != self.admission.body.birth.action.id
            || action.tick != self.admission.body.birth.action.tick
            || action.class != ActionClass::Infallible
            || action.kind != ActionKind::Actor
            || action.chunk_transition
            || action.actor_chunk != Some(self.actor_chunk)
            || action.semantic_intent.is_some()
            || !action.dependencies.is_empty()
            || !action.counterfactual_conditions.is_empty()
            || !action.counterfactual_effects.is_empty()
            || !action.presentation.is_empty()
            || action.edit_authority.is_some()
            || action.player_leases != self.lease.into_iter().collect::<Vec<_>>()
            || action.player_chunks
                != self
                    .lease
                    .into_iter()
                    .map(|lease| (lease.key, self.actor_chunk))
                    .collect::<Vec<_>>()
            || action.effects.as_slice()
                != [Effect::EntityAdmission {
                    target: self.admission.target(),
                    body: self.admission.body.clone(),
                    inventory: self.admission.inventory.clone(),
                }]
            || action.undo != [UndoProcedure::RestoreWrittenTargets]
        {
            return false;
        }
        let mut expected = vec![Condition {
            target: self.admission.target(),
            expected: Expected::IssuerEntityAdmission(Box::new(self.clone())),
        }];
        expected.extend(
            self.admission
                .inventory
                .entries(self.admission.chunk, self.admission.body.id)
                .map(|(target, _)| Condition {
                    target,
                    expected: Expected::Missing,
                }),
        );
        let chunks = [self.actor_chunk, self.admission.chunk]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        action.conditions == expected
            && action.stakeholders
                == chunks
                    .into_iter()
                    .map(Stakeholder::Chunk)
                    .collect::<Vec<_>>()
    }

    pub fn validate_context<S: ActionState>(
        &self,
        state: &S,
        action: &Action,
    ) -> Result<(), ApplyFailure> {
        if !self.guards_valid(action) {
            return Err(ApplyFailure::InvalidAction);
        }
        state.issuer_entity_admission_authority(
            ActionRef {
                id: action.id,
                tick: action.tick,
            },
            self,
        )?;
        state.issuer_entity_initialization_matches(self)?;
        let layout = state.entity_inventory_layout(&self.admission.body)?;
        let expected = layout
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let supplied = self
            .admission
            .inventory
            .cells
            .iter()
            .map(|cell| match &cell.value {
                crate::canonical_action::Value::Inventory(item) if cell.revision == 0 => {
                    Ok(item.loc)
                }
                _ => Err(ApplyFailure::InvalidAction),
            })
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        if expected != supplied || supplied.len() != self.admission.inventory.cells.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }

    pub fn capture_action(&self, client_origin: ClientOrigin) -> Result<Action, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let reference = self.admission.body.birth.action;
        let mut conditions = vec![Condition {
            target: self.admission.target(),
            expected: Expected::IssuerEntityAdmission(Box::new(self.clone())),
        }];
        conditions.extend(
            self.admission
                .inventory
                .entries(self.admission.chunk, self.admission.body.id)
                .map(|(target, _)| Condition {
                    target,
                    expected: Expected::Missing,
                }),
        );
        let action = Action {
            edit_authority: None,
            id: reference.id,
            tick: reference.tick,
            class: ActionClass::Infallible,
            chunk_transition: false,
            actor_chunk: Some(self.actor_chunk),
            player_leases: self.lease.into_iter().collect(),
            player_chunks: self
                .lease
                .into_iter()
                .map(|lease| (lease.key, self.actor_chunk))
                .collect(),
            stakeholders: [self.actor_chunk, self.admission.chunk]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .map(Stakeholder::Chunk)
                .collect(),
            dependencies: Vec::new(),
            conditions,
            effects: vec![Effect::EntityAdmission {
                target: self.admission.target(),
                body: self.admission.body.clone(),
                inventory: self.admission.inventory.clone(),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            client_origin,
            presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }
}

pub fn witness(action: &Action) -> Option<&IssuerAdmissionWitness> {
    let mut witnesses =
        action
            .conditions
            .iter()
            .filter_map(|condition| match &condition.expected {
                Expected::IssuerEntityAdmission(witness)
                    if condition.target == witness.admission.target() =>
                {
                    Some(witness.as_ref())
                }
                _ => None,
            });
    let witness = witnesses.next()?;
    witnesses.next().is_none().then_some(witness)
}

pub fn permits_admission(
    action: &Action,
    target: Target,
    body: &crate::entity_body::EntityBody,
    inventory: &crate::entity_inventory_admission::EntityInventoryAdmission,
) -> bool {
    witness(action).is_some_and(|witness| {
        witness.guards_valid(action)
            && witness.admission.target() == target
            && witness.admission.body == *body
            && witness.admission.inventory == *inventory
    })
}
