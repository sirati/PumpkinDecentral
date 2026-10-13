use pumpkin_data::block_properties::TargetProperties;
use pumpkin_data::entity::EntityType;
use pumpkin_macros::pumpkin_block;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockFlags;

use crate::block::{
    BlockBehaviour, EmitsRedstonePowerArgs, GetRedstonePowerArgs, OnPlaceArgs, OnProjectileHitArgs,
    OnScheduledTickArgs, OnStateReplacedArgs,
};

fn get_redstone_strength(hit_pos: &Vector3<f64>) -> u8 {
    let dist_x = ((hit_pos.x - hit_pos.x.floor()) - 0.5).abs();
    let dist_y = ((hit_pos.y - hit_pos.y.floor()) - 0.5).abs();
    let dist_z = ((hit_pos.z - hit_pos.z.floor()) - 0.5).abs();

    let distance = if dist_x >= dist_y && dist_x >= dist_z {
        dist_y.max(dist_z)
    } else if dist_y >= dist_z {
        dist_x.max(dist_z)
    } else {
        dist_x.max(dist_y)
    };

    let norm = ((0.5 - distance) / 0.5).clamp(0.0, 1.0);
    let power = (15.0 * norm).ceil() as u8;
    power.clamp(1, 15)
}

#[pumpkin_block("minecraft:target")]
pub struct TargetBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for TargetBlock {
    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        true
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = TargetProperties::from_state_id(args.state.id);
        props.r#power
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = TargetProperties::from_state_id(args.state.id);
        props.r#power
    }

    fn on_projectile_hit(&self, args: OnProjectileHitArgs<'_>) {
        if let Some(owner_id) = args.projectile.get_owner_id()
            && let Some(player) = args.world.get_player_by_id(owner_id)
        {
            player.increment_stat(
                pumpkin_data::statistic::StatisticCategory::Custom,
                pumpkin_data::statistic::CustomStatistic::TargetHit as i32,
                1,
            );
        }
        if !args
            .world
            .is_block_tick_scheduled(args.position, args.block)
        {
            let power = get_redstone_strength(args.hit_pos);
            let mut props = TargetProperties::from_state_id(args.state.id);
            props.r#power = power;
            args.world.set_block_state(
                args.position,
                props.to_state_id(args.block),
                BlockFlags::NOTIFY_ALL,
            );
            let entity_type = args.projectile.get_entity().entity_type;
            let delay = if entity_type == &EntityType::ARROW
                || entity_type == &EntityType::SPECTRAL_ARROW
            {
                20
            } else {
                8
            };
            args.world
                .schedule_block_tick(args.block, *args.position, delay, TickPriority::Normal);
        }
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        let state = args.world.get_block_state_id(args.position);
        let mut properties = TargetProperties::from_state_id(state);
        if properties.power == 0 {
            return Some(Vec::new());
        }
        properties.power = 0;
        Some(vec![
            crate::block::neighbor::BlockCallbackEffect::SetBlock {
                position: *args.position,
                state: properties.to_state_id(args.block),
                flags: BlockFlags::NOTIFY_ALL,
            },
        ])
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        crate::block::neighbor::apply_legacy_replaced_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::{BlockCallbackEffect, BlockCallbackEvent};
        match args.event {
            BlockCallbackEvent::Scheduled => {
                self.scheduled_tick_intent(crate::block::ScheduledTickIntentArgs {
                    world: args.world,
                    block: args.block,
                    position: args.position,
                })
            }
            BlockCallbackEvent::StateReplaced { old_state, moved } => {
                if !moved && TargetProperties::from_state_id(old_state).power > 0 {
                    Some(vec![BlockCallbackEffect::NotifyNeighbors {
                        position: *args.position,
                        except: None,
                    }])
                } else {
                    Some(Vec::new())
                }
            }
            BlockCallbackEvent::Placed { .. }
            | BlockCallbackEvent::Prepare { .. }
            | BlockCallbackEvent::Neighbor { .. } => Some(Vec::new()),
        }
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> pumpkin_data::BlockStateId {
        let mut props = TargetProperties::default(args.block);
        props.r#power = 0;
        props.to_state_id(args.block)
    }
}
