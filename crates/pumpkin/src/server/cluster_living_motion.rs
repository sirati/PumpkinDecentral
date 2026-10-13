use std::collections::BTreeMap;
use std::sync::Arc;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{
    Action, ActionState, ApplyFailure, ChunkPart, LivePlayerLease, SpatialEpoch, Target, TruthView,
};
use pumpkin_cluster::combat_action::{
    CombatIntent, CombatStatisticIntent, CombatStatisticKind, FoodExhaustionIntent,
};
use pumpkin_cluster::living_property::LivingProperty;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;

pub(super) struct FrozenMotionAction {
    pub lease: LivePlayerLease,
    pub tick: TickStamp,
    chunk: ChunkKey,
    intent: CombatIntent,
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    draft: Option<ActorActionDraft>,
}

impl FrozenMotionAction {
    pub(super) fn capture(
        server: Arc<super::Server>,
        owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        lease: LivePlayerLease,
        chunk: ChunkKey,
        epoch: SpatialEpoch,
        tick: TickStamp,
        increments: &[(u16, u32, u32)],
        exhaustion: u32,
        previous: super::cluster_world_actor::ActorSpatialState,
        current: super::cluster_world_actor::ActorSpatialState,
        draft: Option<ActorActionDraft>,
    ) -> Result<Self, ApplyFailure> {
        let source = Target::Chunk {
            key: chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let landed = !previous.frame.effective.on_ground && current.frame.effective.on_ground;
        let position = current.frame.effective.position;
        let block = pumpkin_cluster::protocol::BlockPos {
            x: position[0].floor() as i32,
            y: (position[1] - 0.2).floor() as i32,
            z: position[2].floor() as i32,
        };
        let landing = Target::Chunk {
            key: ChunkKey::new(chunk.world, block.x.div_euclid(16), block.z.div_euclid(16)),
            part: ChunkPart::Block(block),
            inventory: None,
        };
        let snapshot = super::cluster_world_callback_snapshot::CallbackTickSnapshot::capture(
            server.clone(),
            owners,
                tick,
            &Default::default(),
            &if landed {
                vec![source, landing]
            } else {
                vec![source]
            },
            &[lease],
            &[],
        )?;
        let mut intent = CombatIntent {
            damage_contacts: Vec::new(),
            falls: Vec::new(),
            damage_parent: None,
            environment: Vec::new(),
            exhaustions: vec![FoodExhaustionIntent {
                lease,
                chunk,
                epoch,
                tick,
                amount_milli: exhaustion,
            }],
            ticks: Vec::new(),
            damages: Vec::new(),
            attacks: Vec::new(),
            vehicle_attacks: Vec::new(),
            attack_clock: None,
            statistics: increments
                .iter()
                .map(|&(category, key, amount)| CombatStatisticIntent {
                    target: Target::Chunk {
                        key: chunk,
                        part: ChunkPart::LivingProperty {
                            actor: ActorId::Player(lease.gid),
                            property: LivingProperty::Statistic { category, key },
                        },
                        inventory: None,
                    },
                    damage_targets: Vec::new(),
                    kind: CombatStatisticKind::Increment(amount),
                })
                .collect(),
            item_transforms: Vec::new(),
            held_attack: None,
            equipment: Vec::new(),
        };
        if landed {
            let producer = pumpkin_cluster::realm_completion::PeerGeneration {
                server: lease.gid.server,
                generation: *snapshot
                    .peer_incarnations()
                    .get(&lease.gid.server)
                    .ok_or(ApplyFailure::InvalidAction)?,
            };
            let contact = pumpkin_cluster::item_contact::ContactObservation::player(
                lease,
                producer,
                epoch,
                chunk,
                current.frame.effective,
                0.3,
                if current.sneaking { 1.5 } else { 1.8 },
                tick,
            )
            .ok_or(ApplyFailure::InvalidAction)?;
            let worlds = server.worlds.load();
            let world = worlds
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == chunk.world)
                .ok_or(ApplyFailure::InvalidAction)?;
            intent
                .falls
                .push(pumpkin_cluster::combat_action::FallDamageIntent {
                    contact,
                    distance: previous.fall_distance.to_bits(),
                    landing,
                    jump_boost_effect: u16::from(pumpkin_data::effect::StatusEffect::JUMP_BOOST.id),
                    damage_enabled: world.level_info.load().game_rules.fall_damage,
                    sneaking: current.sneaking,
                    armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
                    toughness_attribute: u16::from(
                        pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id,
                    ),
                });
        }
        if !intent.is_valid(tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(Self {
            lease,
            tick,
            chunk,
            intent,
            snapshot,
            draft,
        })
    }

    pub(super) fn materialize(
        &self,
        id: pumpkin_cluster::canonical_action::ActionId,
    ) -> Result<(Action, ActorActionDraft), ApplyFailure> {
        let mut draft = self.draft.clone().unwrap_or_else(|| {
            ActorActionDraft::new(
                ActorId::Player(self.lease.gid),
                self.lease.incarnation,
                self.tick,
                4096,
                16384,
            )
        });
        let view = draft.view(self.snapshot.as_ref());
        let values = self
            .intent
            .targets()
            .map(|target| {
                view.read(TruthView::Local, target)
                    .map(|cell| (target, cell))
                    .ok_or(ApplyFailure::Condition(target))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let action = self
            .intent
            .capture_with(&view, id, self.chunk, &values, Vec::new())?;
        draft
            .issue(self.snapshot.as_ref(), action)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        Ok((draft.actions().last().unwrap().clone(), draft))
    }
}
