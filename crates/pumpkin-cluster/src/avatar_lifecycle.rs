use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{ActionId, LivePlayerLease, SpatialEpoch};
use crate::item_action::ActorLivingState;
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerInventoryAdmission {
    pub imported_revision: u64,
    pub cells: Vec<crate::canonical_action::Cell>,
    pub supplemental: Vec<PlayerVirtualInventoryAdmission>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerVirtualInventoryAdmission {
    pub kind: u16,
    pub imported_revision: u64,
    pub cells: Vec<crate::canonical_action::Cell>,
}

pub const PLAYER_CRAFTING_INVENTORY_KIND: u16 = 13;
pub const PLAYER_ENDER_INVENTORY_KIND: u16 = 0x100;

impl PlayerInventoryAdmission {
    pub fn slots() -> impl Iterator<Item = crate::inventory::InvLoc> {
        use crate::inventory::*;
        (0..36)
            .map(|slot| InvLoc::new(INV_MAIN, slot))
            .chain((0..4).map(|slot| InvLoc::new(INV_ARMOR, slot)))
            .chain([
                InvLoc::new(INV_OFFHAND, 0),
                InvLoc::new(INV_CURSOR, 0),
                InvLoc::new(INV_SELECTED, 0),
            ])
    }

    pub fn valid(&self) -> bool {
        use crate::canonical_action::Value;
        use crate::inventory::*;
        if self.cells.len() != 43 {
            return false;
        }
        let mut slots = Self::slots().collect::<std::collections::BTreeSet<_>>();
        for cell in &self.cells {
            if cell.revision != 0 {
                return false;
            }
            let Value::Inventory(item) = &cell.value else {
                return false;
            };
            if !slots.remove(&item.loc) || item.nbt.len() > INVENTORY_MAX_NBT_BYTES {
                return false;
            }
            if item.loc.inv == INV_SELECTED {
                if !(1..=9).contains(&item.item) || item.count != 1 || !item.nbt.is_empty() {
                    return false;
                }
            } else if item.count > INV_MAX_STACK
                || (item.count == 0) != (item.item == 0)
                || item.is_empty() && !item.nbt.is_empty()
            {
                return false;
            }
        }
        if !slots.is_empty() || self.supplemental.len() != 2 {
            return false;
        }
        let mut kinds = std::collections::BTreeSet::new();
        self.supplemental.iter().all(|root| {
            let size = match root.kind {
                PLAYER_CRAFTING_INVENTORY_KIND => 5,
                PLAYER_ENDER_INVENTORY_KIND => 27,
                _ => return false,
            };
            let mut slots = (0..size).collect::<std::collections::BTreeSet<u16>>();
            kinds.insert(root.kind)
                && root.cells.len() == size as usize
                && root.cells.iter().all(|cell| {
                    if cell.revision != 0 {
                        return false;
                    }
                    let Value::Inventory(item) = &cell.value else {
                        return false;
                    };
                    item.loc.inv == INV_MAIN
                        && slots.remove(&item.loc.slot)
                        && item.count <= INV_MAX_STACK
                        && (item.item == 0) == (item.count == 0)
                        && item.nbt.len() <= INVENTORY_MAX_NBT_BYTES
                        && (!item.is_empty() || item.nbt.is_empty())
                })
                && slots.is_empty()
        })
    }

    pub fn targets(
        &self,
        lease: LivePlayerLease,
    ) -> impl Iterator<Item = crate::canonical_action::Target> + '_ {
        self.entries(lease).map(|(target, _)| target)
    }

    pub fn entries(
        &self,
        lease: LivePlayerLease,
    ) -> impl Iterator<
        Item = (
            crate::canonical_action::Target,
            &crate::canonical_action::Cell,
        ),
    > + '_ {
        use crate::canonical_action::*;
        self.cells
            .iter()
            .filter_map(move |cell| match &cell.value {
                Value::Inventory(item) => Some(Target::Player {
                    key: lease.key,
                    lease: Some(lease),
                    inventory: Some(InventoryId::Player {
                        inv: item.loc.inv,
                        slot: item.loc.slot,
                    }),
                })
                .map(|target| (target, cell)),
                _ => None,
            })
            .chain(self.supplemental.iter().flat_map(move |root| {
                root.cells.iter().filter_map(move |cell| match &cell.value {
                    Value::Inventory(item) => Some((
                        Target::Player {
                            key: lease.key,
                            lease: Some(lease),
                            inventory: Some(InventoryId::Virtual {
                                host: lease.gid.server,
                                kind: root.kind,
                                session: 0,
                                inv: item.loc.inv,
                                slot: item.loc.slot,
                            }),
                        },
                        cell,
                    )),
                    _ => None,
                })
            }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvatarLifecycleIntent {
    Admission {
        lease: LivePlayerLease,
        install_generation: u64,
        owner: PeerGeneration,
        initial: ActorLivingState,
        inventory: PlayerInventoryAdmission,
    },
    Retirement {
        lease: LivePlayerLease,
        install_generation: u64,
        owner: PeerGeneration,
    },
}

impl AvatarLifecycleIntent {
    pub fn lease(&self) -> LivePlayerLease {
        match self {
            Self::Admission { lease, .. } | Self::Retirement { lease, .. } => *lease,
        }
    }

    pub fn valid(&self, id: ActionId, tick: TickStamp, chunk: ChunkKey) -> bool {
        let lease = self.lease();
        let (generation, owner) = match self {
            Self::Admission {
                install_generation,
                owner,
                ..
            }
            | Self::Retirement {
                install_generation,
                owner,
                ..
            } => (*install_generation, *owner),
        };
        if generation == 0
            || owner.generation == 0
            || owner.server != lease.gid.server
            || lease.incarnation == 0
            || lease.key.0.is_nil()
            || id.actor != ActorId::Player(lease.gid)
            || id.incarnation != lease.incarnation
        {
            return false;
        }
        match self {
            Self::Admission {
                initial, inventory, ..
            } => {
                let Some(avatar) = initial.avatar.as_ref() else {
                    return false;
                };
                let position = avatar.position.map(f64::from_bits);
                inventory.valid()
                    && initial.belongs_to(ActorId::Player(lease.gid))
                    && avatar.is_valid()
                    && avatar.spatial_epoch
                        == (SpatialEpoch::Admission {
                            lease,
                            admitted_at: tick,
                        })
                    && avatar.world == chunk.world
                    && (position[0].floor() as i32).div_euclid(16) == chunk.x()
                    && (position[2].floor() as i32).div_euclid(16) == chunk.z()
            }
            Self::Retirement { .. } => true,
        }
    }
}

pub fn action(
    id: ActionId,
    tick: TickStamp,
    chunk: ChunkKey,
    intent: AvatarLifecycleIntent,
    retirement_before: Option<crate::canonical_action::Cell>,
) -> Option<crate::canonical_action::Action> {
    use crate::canonical_action::*;
    if !intent.valid(id, tick, chunk) {
        return None;
    }
    let lease = intent.lease();
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let dependencies = retirement_before
        .as_ref()
        .and_then(|cell| match &cell.value {
            Value::Living(state) => state.avatar.as_ref().map(|avatar| avatar.spatial_epoch),
            _ => None,
        })
        .and_then(|epoch| match epoch {
            SpatialEpoch::Action(reference) => Some(reference),
            _ => None,
        })
        .into_iter()
        .collect();
    let conditions = match &intent {
        AvatarLifecycleIntent::Admission { .. } if retirement_before.is_none() => Vec::new(),
        AvatarLifecycleIntent::Retirement { .. } => {
            let before = retirement_before?;
            if !matches!(before.value, Value::Living(ref state) if state.belongs_to(ActorId::Player(lease.gid)))
            {
                return None;
            }
            Vec::new()
        }
        _ => return None,
    };
    let admission = matches!(&intent, AvatarLifecycleIntent::Admission { .. });
    let action = Action {
        edit_authority: None,
        id,
        tick,
        class: ActionClass::Infallible,
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: vec![lease],
        player_chunks: if admission {
            vec![(lease.key, chunk)]
        } else {
            Vec::new()
        },
        stakeholders: if admission {
            vec![Stakeholder::Chunk(chunk), Stakeholder::Player(lease.key)]
        } else {
            vec![Stakeholder::Chunk(chunk)]
        },
        dependencies,
        conditions,
        effects: vec![Effect::PlayerAvatarLifecycle { target, intent }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::PlayerAdministration,
        presentation: Vec::new(),
        client_origin: ClientOrigin::None,
    };
    action.validate().ok()?;
    Some(action)
}
