use std::collections::BTreeSet;
use std::sync::Arc;

use pumpkin_cluster::canonical_action::{
    ActionProposal, ActionState, ApplyFailure, Cell, ChunkPart, ClaimedAction, LivePlayerLease,
    Target, TruthView, Value,
};
use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
use pumpkin_cluster::item_action::ItemActionKind;
use pumpkin_cluster::protocol::{ChunkKey, WorldId};
use pumpkin_data::{attributes::Attributes, item::Item};

use super::cluster_item_raycast::{FluidRayPolicy, ItemRayCapture};
use super::cluster_world_callback_snapshot::CallbackTickSnapshot;

pub(super) struct FrozenBucketUse {
    original: Arc<CallbackTickSnapshot>,
    pub lease: LivePlayerLease,
    pub source: InventorySlotBinding,
    pub source_cell: Cell,
    source_metadata: super::cluster_item_use::ItemUseSourceMetadata,
    pub world: WorldId,
    pub start: [f64; 3],
    pub end: [f64; 3],
    pub fluids: FluidRayPolicy,
    pub rules: pumpkin_cluster::world_rules::WorldRulesWitness,
    policy: pumpkin_cluster::world_rules::WorldRulesSnapshot,
    required: Vec<Target>,
    ray_targets: Vec<Target>,
}

pub(super) fn supports(proposal: &ActionProposal) -> bool {
    matches!(&proposal.claim, ClaimedAction::ItemUse(claim)
        if matches!(claim.intent.kind, ItemActionKind::Use { .. })
        && [Item::BUCKET.id, Item::WATER_BUCKET.id, Item::LAVA_BUCKET.id, Item::POWDER_SNOW_BUCKET.id]
            .contains(&claim.intent.expected_item.item))
}

impl FrozenBucketUse {
    pub(super) fn capture<S: ActionState>(
        original: Arc<CallbackTickSnapshot>,
        view: &S,
        proposal: &ActionProposal,
    ) -> Result<Self, ApplyFailure> {
        if !supports(proposal) {
            return Err(ApplyFailure::InvalidAction);
        }
        let ClaimedAction::ItemUse(claim) = &proposal.claim else {
            unreachable!()
        };
        let metadata =
            super::cluster_item_use::capture_source_metadata(original.as_ref(), proposal)?;
        let (lease, source, source_cell, selected, _) =
            super::cluster_item_use::normalize_captured_source(view, proposal, &metadata)?;
        let Value::Inventory(item) = &source_cell.value else {
            return Err(ApplyFailure::Inventory(source.target));
        };
        let fluids = if item.item == Item::BUCKET.id {
            FluidRayPolicy::Source
        } else if [
            Item::WATER_BUCKET.id,
            Item::LAVA_BUCKET.id,
            Item::POWDER_SNOW_BUCKET.id,
        ]
        .contains(&item.item)
        {
            FluidRayPolicy::None
        } else {
            return Err(ApplyFailure::Inventory(source.target));
        };
        if item.count == 0 {
            return Err(ApplyFailure::Inventory(source.target));
        }
        let actor = Target::Chunk {
            key: claim.intent.actor_chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let body = view
            .read(TruthView::Local, actor)
            .ok_or(ApplyFailure::Condition(actor))?;
        let Value::Living(living) = &body.value else {
            return Err(ApplyFailure::Type(actor));
        };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(actor))?;
        if living.combat.health_milli == 0 || avatar.game_mode >= 2 {
            return Err(ApplyFailure::InvalidAction);
        }
        let projection = original
            .player_projection(lease.gid)
            .filter(|projection| projection.chunk == claim.intent.actor_chunk)
            .ok_or(ApplyFailure::Condition(actor))?;
        let frame = projection.frame.effective;
        let reach_id = u16::from(Attributes::BLOCK_INTERACTION_RANGE.id);
        let reach = living
            .combat
            .effective_attributes
            .get(&reach_id)
            .copied()
            .map(f64::from_bits)
            .map(|base| living.attribute(reach_id, base))
            .ok_or(ApplyFailure::InvalidAction)?;
        let mut pose = super::cluster_living_metadata::player_pose(living);
        if projection.gliding {
            pose = pumpkin_data::entity::EntityPose::FallFlying;
        } else if projection.crawling || projection.swimming {
            pose = pumpkin_data::entity::EntityPose::Swimming;
        } else if projection.sneaking {
            pose = pumpkin_data::entity::EntityPose::Crouching;
        }
        let eye =
            f64::from(super::cluster_living_metadata::player_dimensions(living, pose).eye_height);
        let (start, end) = super::cluster_item_raycast::segment(
            frame.position,
            eye,
            frame.yaw,
            frame.pitch,
            reach,
        )?;
        let world = claim.intent.actor_chunk.world;
        let (rules, policy) = original
            .world_rules_at(world, proposal.tick)
            .ok_or(ApplyFailure::InvalidAction)?;
        let policy = policy.clone();
        let ray_targets = super::cluster_item_raycast::required_targets_with_rules(
            world, start, end, fluids, &policy,
        )?;
        let mut required = ray_targets
            .iter()
            .copied()
            .chain([actor, source.target, selected.target])
            .collect::<BTreeSet<_>>();
        if fluids == FluidRayPolicy::None {
            let maximum_y = policy
                .min_y
                .checked_add(i32::try_from(policy.height).map_err(|_| ApplyFailure::InvalidAction)?)
                .ok_or(ApplyFailure::InvalidAction)?;
            for target in &ray_targets {
                let Target::Chunk {
                    part: ChunkPart::Block(position),
                    ..
                } = target
                else {
                    continue;
                };
                for direction in pumpkin_data::BlockDirection::all() {
                    let delta = direction.to_offset();
                    let position = pumpkin_cluster::protocol::BlockPos {
                        x: position
                            .x
                            .checked_add(delta.x)
                            .ok_or(ApplyFailure::InvalidAction)?,
                        y: position
                            .y
                            .checked_add(delta.y)
                            .ok_or(ApplyFailure::InvalidAction)?,
                        z: position
                            .z
                            .checked_add(delta.z)
                            .ok_or(ApplyFailure::InvalidAction)?,
                    };
                    if position.y >= policy.min_y && position.y < maximum_y {
                        required.insert(Target::Chunk {
                            key: ChunkKey::new(
                                world,
                                position.x.div_euclid(16),
                                position.z.div_euclid(16),
                            ),
                            part: ChunkPart::Block(position),
                            inventory: None,
                        });
                    }
                }
            }
        }
        for (inventory, count) in [
            (pumpkin_cluster::inventory::INV_MAIN, 36_u16),
            (pumpkin_cluster::inventory::INV_OFFHAND, 1_u16),
        ] {
            for slot in 0..count {
                required.insert(Target::Player {
                    key: lease.key,
                    lease: Some(lease),
                    inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player {
                        inv: inventory,
                        slot,
                    }),
                });
            }
        }
        for property in [
            pumpkin_cluster::living_property::LivingProperty::GameMode,
            pumpkin_cluster::living_property::LivingProperty::Health,
            pumpkin_cluster::living_property::LivingProperty::Attribute(reach_id),
            pumpkin_cluster::living_property::LivingProperty::PermanentAttributeModifiers(reach_id),
            pumpkin_cluster::living_property::LivingProperty::Attribute(u16::from(
                Attributes::SCALE.id,
            )),
            pumpkin_cluster::living_property::LivingProperty::PermanentAttributeModifiers(
                u16::from(Attributes::SCALE.id),
            ),
        ] {
            required.insert(Target::Chunk {
                key: claim.intent.actor_chunk,
                part: ChunkPart::LivingProperty {
                    actor: proposal.id.actor,
                    property,
                },
                inventory: None,
            });
        }
        Ok(Self {
            original,
            lease,
            source,
            source_cell,
            source_metadata: metadata,
            world,
            start,
            end,
            fluids,
            rules,
            policy,
            required: required.into_iter().collect(),
            ray_targets,
        })
    }

    pub(super) fn targets(&self) -> &[Target] {
        &self.required
    }
    pub(super) fn chunks(&self) -> Vec<ChunkKey> {
        self.required
            .iter()
            .filter_map(|target| match target {
                Target::Chunk { key, .. } => Some(*key),
                _ => None,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub(super) fn source_readiness<S: ActionState>(&self, view: &S) -> Result<(), ApplyFailure> {
        for target in &self.required {
            if !view.available(TruthView::Local, *target) {
                return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
            }
            if view.read(TruthView::Local, *target).is_none() {
                return Err(ApplyFailure::Condition(*target));
            }
        }
        super::cluster_item_raycast::source_readiness(view, &self.ray_targets)
    }
    pub(super) fn capture_ray<S: ActionState>(
        &self,
        view: &S,
    ) -> Result<ItemRayCapture, ApplyFailure> {
        self.source_readiness(view)?;
        super::cluster_item_raycast::capture_with_rules(
            view,
            self.world,
            self.start,
            self.end,
            self.fluids,
            &self.policy,
        )
    }
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        let (records, bytes) = self.source_metadata.captured_resource_usage()?;
        Ok((
            records
                .checked_add(self.required.len())
                .ok_or(ApplyFailure::Capacity)?,
            bytes
                .checked_add(
                    self.required
                        .len()
                        .saturating_mul(std::mem::size_of::<Target>()),
                )
                .ok_or(ApplyFailure::Capacity)?,
        ))
    }
    pub(super) fn original(&self) -> &Arc<CallbackTickSnapshot> {
        &self.original
    }
}
