use super::attack::attack_and_replicate;
#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    #[expect(clippy::too_many_lines)]
    pub fn handle_interact(
        &self,
        player: &Arc<Player>,
        interact: &SInteract,
        server: &Arc<Server>,
    ) {
        if !player.has_client_loaded() {
            return;
        }
        player.update_last_action_time();
        let entity_id = interact.entity_id;

        let sneaking = interact.sneaking;
        let Ok(action) = ActionType::try_from(interact.r#type.0) else {
            self.try_kick(&TextComponent::text("Invalid action type"));
            return;
        };
        if server.advanced_config.cluster.enabled {
            if !crate::server::cluster_controller::submit_patch(
                server,
                player,
                crate::server::cluster_controller::ControllerPatch::Sneak(sneaking),
            ) {
                return;
            }
            let Some((target, target_chunk)) =
                crate::server::cluster_inventory_protocol::actor_for_entity_id(player, entity_id.0)
            else {
                return;
            };
            let interaction = if action == ActionType::Attack {
                pumpkin_cluster::item_action::ActorItemInteraction::Attack
            } else {
                pumpkin_cluster::item_action::ActorItemInteraction::Interact
            };
            let hit = match interact.target_position {
                Some(position)
                    if position.x.is_finite()
                        && position.y.is_finite()
                        && position.z.is_finite()
                        && position.x >= f64::from(f32::MIN)
                        && position.x <= f64::from(f32::MAX)
                        && position.y >= f64::from(f32::MIN)
                        && position.y <= f64::from(f32::MAX)
                        && position.z >= f64::from(f32::MIN)
                        && position.z <= f64::from(f32::MAX) =>
                {
                    Some([position.x as f32, position.y as f32, position.z as f32])
                }
                Some(_) => return,
                None => None,
            };
            let hand = interact.hand.map_or(0, |hand| hand.0 as u8);
            crate::server::cluster_inventory_protocol::stage_item_action(
                player,
                hand,
                crate::server::cluster_inventory_protocol::StagedItemActionKind::UseOnActor {
                    target,
                    target_chunk,
                    hit,
                    interaction,
                },
                None,
            );
            return;
        }

        let player_entity = &player.get_entity();
        if player_entity.is_sneaking() != sneaking {
            player_entity.set_sneaking(sneaking);
        }

        // Resolve the target entity for the event
        let world = player_entity.world.load_full();
        let player_target = world.get_player_by_id(entity_id.0);
        let target: Option<Arc<dyn EntityBase>> = player_target
            .as_ref()
            .map(|p| Arc::clone(p) as Arc<dyn EntityBase>)
            .or_else(|| world.get_entity_by_id(entity_id.0));

        if let Some(target) = target {
            if player.gamemode.load() == GameMode::Spectator {
                player.camera_target_id.store(Some(entity_id.0));
                player.try_send_client_packet(&CSetCamera::new(entity_id));
                return;
            }
            send_cancellable_blocking! {{
                server;
                PlayerInteractEntityEvent::new(
                    player,
                    Arc::clone(&target),
                    action,
                    interact.target_position,
                    sneaking,
                );

                'after: {
                    match event.action {
                        ActionType::Attack => {
                            let config = &server.advanced_config.pvp;
                            if !config.enabled {
                                return;
                            }

                            if entity_id.0 == player.entity_id() {
                                self.try_kick(&TextComponent::translate_cross(translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED, translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED, []));
                                return;
                            }

                            if let Some(player_victim) = &player_target {
                                if player_victim.living_entity.health.load() <= 0.0 {
                                    return;
                                }
                                if config.protect_creative
                                    && player_victim.gamemode.load() == GameMode::Creative
                                {
                                    world
                                        .play_sound(
                                            Sound::EntityPlayerAttackNodamage,
                                            SoundCategory::Players,
                                            &player_victim.position(),
                                        )
                                        ;
                                    return;
                                }
                            }
                            attack_and_replicate(
                                player,
                                &event.target,
                                player_target.is_some()
                                    || crate::server::cluster_ghost::remote_player_gid(
                                        event.target.as_ref(),
                                    )
                                    .is_some(),
                                player_target
                                    .as_ref()
                                    .and_then(|victim| victim.cluster_gid())
                                    .or_else(|| {
                                        crate::server::cluster_ghost::remote_player_gid(
                                            event.target.as_ref(),
                                        )
                                    }),
                                server,
                            );
                        }
                        ActionType::Interact | ActionType::InteractAt => {
                            if event.action == ActionType::InteractAt
                                && let Some(pos) = interact.target_position
                            {
                                let mut at_event = crate::plugin::api::events::player::player_interact_at_entity::PlayerInteractAtEntityEvent::new(
                                    player.clone(),
                                    entity_id.0,
                                    pos.x,
                                    pos.y,
                                    pos.z,
                                    u8::from(interact.hand.map_or(0, |h| h.0) != 0),
                                );
                                server.plugin_manager.fire_blocking(server, &mut at_event);
                                if at_event.cancelled {
                                    return;
                                }
                            }
                            let mut stack = player.inventory().held_item();

                            let item_id = stack.item.id;
                            let before = stack.clone();
                            let interacted = event.target.interact(player, &mut stack);
                            if !interacted {
                                server
                                    .item_registry
                                    .use_on_entity(&mut stack, player, event.target);
                            }
                            if !stack.are_equal(&before) {
                                player.increment_stat(StatisticCategory::Used, item_id as i32, 1);
                                if before.is_damageable() && stack.is_empty() {
                                    player.increment_stat(
                                        StatisticCategory::Broken,
                                        item_id as i32,
                                        1,
                                    );
                                    player.world().send_entity_status(
                                        player.get_entity(),
                                        equipment_break_status(&EquipmentSlot::MAIN_HAND),
                                        None,
                                    );
                                }
                            }
                            player.inventory().set_held_item(stack);
                        }
                    }
                }
            }}
        } else {
            // Entity not found
            send_cancellable_blocking! {{
                server;
                PlayerInteractUnknownEntityEvent::new(player, entity_id.0, action);

                'after: {
                    if event.action == ActionType::Attack {
                        error!(
                            "Player id {} interacted with entity id {}, which was not found.",
                            player.entity_id(),
                            event.entity_id
                        );
                        self.try_kick(&TextComponent::translate_cross(translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED, translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED, []));
                    }
                }
            }}
        }
    }
}
