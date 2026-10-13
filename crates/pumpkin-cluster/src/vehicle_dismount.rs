use crate::actor::ActorId;
use crate::actor_relation::PlayerDetachPlan;
use crate::canonical_action::*;
use crate::item_contact::ContactIdentity;
use crate::protocol::ChunkKey;
use crate::time::TickStamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DismountClaim {
    pub owner_install_generation: u64,
    pub source_epoch: SpatialEpoch,
    pub mount: ContactIdentity,
    pub vehicle_chunk: ChunkKey,
    pub seat: u8,
}

pub struct FrozenVehicleDismount {
    lease: LivePlayerLease,
    chunk: ChunkKey,
    plan: PlayerDetachPlan,
}

impl FrozenVehicleDismount {
    pub fn capture(
        state: &impl ActionState,
        lease: LivePlayerLease,
        chunk: ChunkKey,
        claim: &DismountClaim,
    ) -> Result<Self, ApplyFailure> {
        if claim.owner_install_generation == 0
            || claim.seat >= 32
            || claim.vehicle_chunk.world != chunk.world
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let player = Target::Chunk {
            key: chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let Some(Cell {
            value: Value::Living(living),
            ..
        }) = state.read(TruthView::Local, player)
        else {
            return Err(ApplyFailure::Condition(player));
        };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(player))?;
        if avatar.spatial_epoch != claim.source_epoch || avatar.mount.as_ref() != Some(&claim.mount)
        {
            return Err(ApplyFailure::Condition(player));
        }
        let ContactIdentity::Entity { id, birth } = &claim.mount else {
            return Err(ApplyFailure::InvalidAction);
        };
        let target = Target::Chunk {
            key: claim.vehicle_chunk,
            part: ChunkPart::Entity(*id),
            inventory: None,
        };
        let Some(Cell {
            value: Value::Entity(body),
            ..
        }) = state.read(TruthView::Local, target)
        else {
            return Err(ApplyFailure::Condition(target));
        };
        if &body.birth != birth
            || body.passengers.get(&claim.seat) != Some(&ContactIdentity::Player(lease))
        {
            return Err(ApplyFailure::Condition(target));
        }
        let plan = crate::actor_relation::capture_player_detach(
            state,
            lease,
            chunk,
            claim.source_epoch,
            &claim.mount,
            claim.vehicle_chunk,
            &body,
        )?;
        Ok(Self { lease, chunk, plan })
    }

    pub fn targets(&self) -> Vec<Target> {
        self.plan
            .conditions
            .iter()
            .map(|condition| condition.target)
            .collect()
    }

    pub fn materialize(&self, id: ActionId, tick: TickStamp) -> Result<Action, ApplyFailure> {
        if id.actor != ActorId::Player(self.lease.gid) || id.incarnation != self.lease.incarnation {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut dependencies = self
            .plan
            .conditions
            .iter()
            .filter_map(|condition| match condition.expected {
                Expected::SpatialEpochEquals(SpatialEpoch::Action(reference)) => Some(reference),
                _ => None,
            })
            .collect::<Vec<_>>();
        dependencies.sort_unstable();
        dependencies.dedup();
        let action = Action {
            edit_authority: None,
            id,
            tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(self.chunk),
            player_leases: vec![self.lease],
            player_chunks: Vec::new(),
            stakeholders: self
                .plan
                .chunks
                .iter()
                .copied()
                .map(Stakeholder::Chunk)
                .collect(),
            dependencies,
            conditions: self.plan.conditions.clone(),
            effects: self.plan.effects.clone(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate()?;
        Ok(action)
    }
}
