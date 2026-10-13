use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountObservation {
    pub actor: crate::action_command::ActorReference,
    pub cut: crate::actor_occupancy::ActorOccupancyCut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorMount {
    pub source: MountObservation,
    pub target: MountObservation,
    pub hand: crate::action_command::Hand,
}
