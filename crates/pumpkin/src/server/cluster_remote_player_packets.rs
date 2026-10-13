use super::cluster_remote_player_presentation::RemotePlayerFrame;

pub(super) fn prepare_remove(
    client: &crate::net::ClientPlatform,
    id: i32,
) -> Result<Vec<bytes::Bytes>, String> {
    match client {
        crate::net::ClientPlatform::Java(client) => Ok(vec![
            crate::net::java::JavaClient::serialize_packet_for_version(
                &pumpkin_protocol::java::client::play::CRemoveEntities::new(&[id.into()]),
                client.version.load(),
            )
            .map_err(|error| error.to_string())?,
        ]),
        crate::net::ClientPlatform::Bedrock(client) => Ok(vec![
            client
                .serialize_packet(
                    &pumpkin_protocol::bedrock::client::remove_actor::CRemoveActor {
                        target_actor_id: i64::from(id).into(),
                    },
                )
                .map_err(|error| error.to_string())?,
        ]),
    }
}

pub(super) fn prepare_frame(
    client: &crate::net::ClientPlatform,
    before: Option<&RemotePlayerFrame>,
    after: &RemotePlayerFrame,
) -> Result<Vec<bytes::Bytes>, String> {
    use pumpkin_protocol::java::client::play::*;
    use pumpkin_util::math::vector3::Vector3;
    let id = after.client_entity_id;
    let spatial = after.spatial.frame.effective;
    let position = Vector3::new(
        spatial.position[0],
        spatial.position[1],
        spatial.position[2],
    );
    let velocity = Vector3::new(
        spatial.velocity[0],
        spatial.velocity[1],
        spatial.velocity[2],
    );
    let mut frames = Vec::new();
    let restored_life = before.is_some_and(|previous| {
        previous.living.combat.health_milli == 0 && after.living.combat.health_milli != 0
    });
    let changed_profile = before.is_some_and(|previous| {
        previous.profile.name != after.profile.name
            || previous.profile.properties != after.profile.properties
            || matches!(client, crate::net::ClientPlatform::Bedrock(_))
                && previous.bedrock_skin != after.bedrock_skin
    });
    if restored_life || changed_profile {
        frames.extend(prepare_remove(client, id)?);
    }
    let before = before.filter(|_| !restored_life && !changed_profile);
    let metadata_changed = before.is_none_or(|previous| {
        !std::sync::Arc::ptr_eq(&previous.living, &after.living)
            || previous.skin_layers != after.skin_layers
            || previous.main_hand != after.main_hand
            || previous.mask_health != after.mask_health
            || previous.health_policy != after.health_policy
    });
    if after.equipment.is_none() {
        return Err("remote player inventory baseline unavailable".to_owned());
    }
    match client {
        crate::net::ClientPlatform::Java(client) => {
            let version = client.version.load();
            if before.is_some_and(|previous| {
                previous.spatial.game_mode != after.spatial.game_mode
                    || previous.profile.listed != after.profile.listed
            }) {
                let actions = [
                    PlayerAction::UpdateGameMode(i32::from(after.spatial.game_mode).into()),
                    PlayerAction::UpdateListed(after.profile.listed),
                ];
                let profiles = [Player {
                    uuid: after.lease.key.0,
                    actions: &actions,
                }];
                frames.push(
                    crate::net::java::JavaClient::serialize_packet_for_version(
                        &CPlayerInfoUpdate::new(
                            (PlayerInfoFlags::UPDATE_GAME_MODE | PlayerInfoFlags::UPDATE_LISTED)
                                .bits(),
                            &profiles,
                        ),
                        version,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            if before.is_none() {
                let properties = after
                    .profile
                    .properties
                    .iter()
                    .map(|property| pumpkin_protocol::Property {
                        name: property.name.clone().into(),
                        value: property.value.clone().into(),
                        signature: property.signature.clone().map(Into::into),
                    })
                    .collect::<Vec<_>>();
                let actions = [
                    PlayerAction::AddPlayer {
                        name: &after.profile.name,
                        properties: &properties,
                    },
                    PlayerAction::UpdateGameMode(i32::from(after.spatial.game_mode).into()),
                    PlayerAction::UpdateListed(after.profile.listed),
                    PlayerAction::UpdateLatency(0.into()),
                ];
                let profiles = [Player {
                    uuid: after.lease.key.0,
                    actions: &actions,
                }];
                frames.push(
                    crate::net::java::JavaClient::serialize_packet_for_version(
                        &CPlayerInfoUpdate::new(
                            (PlayerInfoFlags::ADD_PLAYER
                                | PlayerInfoFlags::UPDATE_GAME_MODE
                                | PlayerInfoFlags::UPDATE_LISTED
                                | PlayerInfoFlags::UPDATE_LATENCY)
                                .bits(),
                            &profiles,
                        ),
                        version,
                    )
                    .map_err(|error| error.to_string())?,
                );
                if version < pumpkin_util::version::JavaMinecraftVersion::V_1_20_2 {
                    frames.push(
                        crate::net::java::JavaClient::serialize_packet_for_version(
                            &CSpawnPlayer {
                                entity_id: id.into(),
                                uuid: after.lease.key.0,
                                name: &after.profile.name,
                                properties: &properties,
                                position,
                                yaw: spatial.yaw,
                                pitch: spatial.pitch,
                            },
                            version,
                        )
                        .map_err(|error| error.to_string())?,
                    );
                } else {
                    frames.push(
                        crate::net::java::JavaClient::serialize_packet_for_version(
                            &CSpawnEntity::new(
                                id.into(),
                                after.lease.key.0,
                                i32::from(pumpkin_data::entity::EntityType::PLAYER.id).into(),
                                position,
                                spatial.pitch.rem_euclid(360.0),
                                spatial.yaw,
                                after.spatial.head_yaw,
                                0.into(),
                                velocity,
                            ),
                            version,
                        )
                        .map_err(|error| error.to_string())?,
                    );
                }
            } else {
                frames.push(
                    crate::net::java::JavaClient::serialize_packet_for_version(
                        &CEntityPositionSync::new(
                            id.into(),
                            position,
                            velocity,
                            spatial.yaw,
                            spatial.pitch,
                            spatial.on_ground,
                        ),
                        version,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            if before.is_none_or(|previous| previous.spatial.head_yaw != after.spatial.head_yaw) {
                frames.push(
                    crate::net::java::JavaClient::serialize_packet_for_version(
                        &CHeadRot::new(
                            id.into(),
                            (after.spatial.head_yaw.rem_euclid(360.0) * 256.0 / 360.0).floor()
                                as u8,
                        ),
                        version,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            if before.is_some_and(|before| {
                before.living.combat.velocity_micro != after.living.combat.velocity_micro
            }) {
                let velocity = after
                    .living
                    .combat
                    .velocity_micro
                    .map(|value| value as f64 / 1_000_000.0);
                frames.push(
                    crate::net::java::JavaClient::serialize_packet_for_version(
                        &CEntityVelocity::new(
                            id.into(),
                            Vector3::new(velocity[0], velocity[1], velocity[2]),
                        ),
                        version,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            if metadata_changed {
                let mut metadata = super::cluster_living_metadata::java_player_presentation(
                    &after.living,
                    version,
                )
                .map_err(|error| error.to_string())?;
                metadata.pop();
                if !after.mask_health || before.is_some_and(|previous| !previous.mask_health) {
                    for (field, value) in [
                        (
                            pumpkin_data::tracked_data::living_entity::DATA_HEALTH_ID,
                            if after.mask_health {
                                20.0
                            } else {
                                after.living.combat.health_milli as f32 / 1000.0
                            },
                        ),
                        (
                            pumpkin_data::tracked_data::player::DATA_PLAYER_ABSORPTION_ID,
                            if after.mask_health {
                                0.0
                            } else {
                                after.living.combat.absorption_milli as f32 / 1000.0
                            },
                        ),
                    ] {
                        if field.id.get(&version) != 255 {
                            Metadata::new(field, value)
                                .write(&mut metadata, &version)
                                .map_err(|error| error.to_string())?;
                        }
                    }
                }
                if let Some(hand) = after.main_hand {
                    for tracked in [
                        pumpkin_data::tracked_data::player::PLAYER_MAIN_HAND,
                        pumpkin_data::tracked_data::player::MAIN_ARM_ID,
                    ] {
                        if tracked.id.get(&version) != 255 {
                            Metadata::new(tracked, hand)
                                .write(&mut metadata, &version)
                                .map_err(|error| error.to_string())?;
                        }
                    }
                }
                if let Some(skin_layers) = after.skin_layers {
                    let tracked = pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMISATION;
                    if tracked.id.get(&version) != 255 {
                        Metadata::new(tracked, skin_layers)
                            .write(&mut metadata, &version)
                            .map_err(|error| error.to_string())?;
                    }
                    let tracked = pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMIZATION_ID;
                    if tracked.id.get(&version) != 255 {
                        Metadata::new(tracked, skin_layers)
                            .write(&mut metadata, &version)
                            .map_err(|error| error.to_string())?;
                    }
                }
                metadata.push(
                    if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 {
                        127
                    } else {
                        255
                    },
                );
                frames.push(
                    crate::net::java::JavaClient::serialize_packet_for_version(
                        &CSetEntityMetadata::new(id.into(), metadata.into_boxed_slice()),
                        version,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            if let Some(equipment) = &after.equipment {
                for (slot, cell) in equipment.iter().enumerate() {
                    if before
                        .and_then(|frame| frame.equipment.as_ref())
                        .is_some_and(|old| old[slot] == *cell)
                    {
                        continue;
                    }
                    if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 && slot == 1 {
                        continue;
                    }
                    let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell)
                        .map_err(|error| format!("{error:?}"))?;
                    frames.push(crate::net::java::JavaClient::serialize_packet_for_version(
                        &CSetEquipment::new(id.into(), vec![(slot as i8, pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer(std::borrow::Cow::Owned(stack)))]), version,
                    ).map_err(|error| error.to_string())?);
                }
            }
            {
                let status = if after.living.combat.health_milli == 0
                    && before.is_none_or(|previous| previous.living.combat.health_milli != 0)
                {
                    Some(3)
                } else if before.is_some_and(|previous| {
                    previous.living.combat.health_milli > after.living.combat.health_milli
                        || previous.living.combat.absorption_milli
                            > after.living.combat.absorption_milli
                }) {
                    Some(2)
                } else {
                    None
                };
                if let Some(status) = status {
                    frames.push(
                        crate::net::java::JavaClient::serialize_packet_for_version(
                            &CEntityStatus::new(id, status),
                            version,
                        )
                        .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            let metadata = metadata_changed.then(|| bedrock_metadata(after));
            if before.is_some_and(|previous| previous.spatial.game_mode != after.spatial.game_mode)
            {
                frames.push(
                    client
                        .serialize_packet(
                            &pumpkin_protocol::bedrock::client::CUpdatePlayerGameType {
                                player_game_type: pumpkin_util::GameMode::try_from(
                                    after.spatial.game_mode,
                                )
                                .map_err(|error| format!("{error:?}"))?
                                .into(),
                                target_player: i64::from(id).into(),
                                tick: 0_u64.into(),
                            },
                        )
                        .map_err(|error| error.to_string())?,
                );
            }
            let gear = after.equipment.as_ref().filter(|equipment| before
                .and_then(|frame| frame.equipment.as_ref()) != Some(*equipment)).map(|equipment| equipment.iter()
                .map(|cell| super::cluster_invsee::item_stack_from_inventory_cell(cell)
                    .map(|stack| pumpkin_protocol::bedrock::network_item::NetworkItemStackDescriptor::from(&stack)))
                .collect::<Result<Vec<_>, _>>()).transpose().map_err(|error| format!("{error:?}"))?;
            if before.is_none() {
                use pumpkin_protocol::bedrock::client::common::*;
                use pumpkin_protocol::bedrock::client::player_list::{
                    CPlayerList, PlayerListEntry,
                };
                let skin = after
                    .bedrock_skin
                    .as_ref()
                    .ok_or("remote player skin baseline unavailable")?;
                frames.push(
                    client
                        .serialize_packet(&CPlayerList {
                            action: CPlayerList::ACTION_ADD,
                            entries: vec![PlayerListEntry {
                                uuid: after.lease.key.0,
                                entity_unique_id: i64::from(id).into(),
                                username: after.profile.name.clone(),
                                xuid: String::new(),
                                platform_chat_id: String::new(),
                                build_platform: BuildPlatform::Unknown,
                                skin: (*skin.0).clone(),
                                is_teacher: false,
                                is_host: false,
                                is_sub_client: false,
                                player_color: [255; 4],
                            }],
                        })
                        .map_err(|error| error.to_string())?,
                );
                let packet = pumpkin_protocol::bedrock::client::add_player::CAddPlayer {
                    uuid: after.lease.key.0,
                    player_name: after.profile.name.clone(),
                    target_runtime_id: (id as u64).into(),
                    platform_chat_id: String::new(),
                    position: position.to_f32_lossy(),
                    velocity: velocity.to_f32_lossy(),
                    rotation: pumpkin_util::math::vector2::Vector2::new(spatial.pitch, spatial.yaw),
                    y_head_rotation: after.spatial.head_yaw,
                    carried_item: gear
                        .as_ref()
                        .map(|gear| gear[0].clone())
                        .unwrap_or_default(),
                    player_game_type: pumpkin_util::GameMode::try_from(after.spatial.game_mode)
                        .map_err(|error| format!("{error:?}"))?
                        .into(),
                    entity_data: bedrock_metadata(after),
                    synced_properties: Default::default(),
                    abilities_data: SerializedAbilitiesData {
                        target_player_raw_id: i64::from(id),
                        player_permissions:
                            pumpkin_protocol::bedrock::client::PlayerPermissionLevel::Visitor,
                        command_permissions:
                            pumpkin_protocol::bedrock::client::CommandPermissionLevel::Any,
                        layers: vec![SerializedAbilitiesDataSerializedLayer {
                            serialized_layer: 0,
                            abilities_set: 0,
                            ability_value: 0,
                            fly_speed: 0.05,
                            vertical_fly_speed: 0.05,
                            walk_speed: 0.1,
                        }],
                    },
                    actor_links: Vec::new(),
                    device_id: String::new(),
                    build_platform: BuildPlatform::Unknown,
                };
                frames.push(
                    client
                        .serialize_packet(&packet)
                        .map_err(|error| error.to_string())?,
                );
            }
            let packet = pumpkin_protocol::bedrock::client::move_player::CMovePlayer::new(
                (id as u64).into(),
                Vector3::new(
                    position.x as f32,
                    position.y as f32
                        + super::cluster_living_metadata::player_eye_height(&after.living),
                    position.z as f32,
                ),
                spatial.pitch,
                spatial.yaw,
                after.spatial.head_yaw,
                pumpkin_protocol::bedrock::client::move_player::CMovePlayer::MODE_NORMAL,
                spatial.on_ground,
                0_u64.into(),
                0,
                0,
                0_u64.into(),
            );
            frames.push(
                client
                    .serialize_packet(&packet)
                    .map_err(|error| error.to_string())?,
            );
            if let Some(metadata) = metadata {
                frames.push(
                    client
                        .serialize_packet(
                            &pumpkin_protocol::bedrock::client::set_actor_data::CSetActorData {
                                target_runtime_id: (id as u64).into(),
                                actor_data: metadata,
                                synced_properties: Default::default(),
                                tick: u64::from(spatial.tick.0).into(),
                            },
                        )
                        .map_err(|error| error.to_string())?,
                );
            }
            if before.is_some_and(|before| {
                before.living.combat.velocity_micro != after.living.combat.velocity_micro
            }) {
                let velocity = after
                    .living
                    .combat
                    .velocity_micro
                    .map(|value| value as f32 / 1_000_000.0);
                frames.push(
                    client
                        .serialize_packet(&pumpkin_protocol::bedrock::client::CSetActorMotion {
                            target_runtime_id: (id as u64).into(),
                            motion: Vector3::new(velocity[0], velocity[1], velocity[2]),
                            tick: u64::from(spatial.tick.0).into(),
                        })
                        .map_err(|error| error.to_string())?,
                );
            }
            if let Some(gear) = gear {
                for slot in 0..2 {
                    if before
                        .and_then(|frame| frame.equipment.as_ref())
                        .zip(after.equipment.as_ref())
                        .is_some_and(|(old, new)| old[slot] == new[slot])
                    {
                        continue;
                    }
                    frames.push(
                        client
                            .serialize_packet(&pumpkin_protocol::bedrock::client::CMobEquipment {
                                target_runtime_id: (id as u64).into(),
                                item: gear[slot].clone(),
                                slot: 0,
                                selected_slot: 0,
                                container_id: if slot == 1 { 120 } else { 0 },
                            })
                            .map_err(|error| error.to_string())?,
                    );
                }
                if !before
                    .and_then(|frame| frame.equipment.as_ref())
                    .zip(after.equipment.as_ref())
                    .is_some_and(|(old, new)| old[2..] == new[2..])
                {
                    frames.push(
                        client
                            .serialize_packet(
                                &pumpkin_protocol::bedrock::client::CMobArmorEquipment {
                                    target_runtime_id: (id as u64).into(),
                                    head: gear[5].clone(),
                                    torso: gear[4].clone(),
                                    legs: gear[3].clone(),
                                    feet: gear[2].clone(),
                                    body: Default::default(),
                                },
                            )
                            .map_err(|error| error.to_string())?,
                    );
                }
            }
            {
                use pumpkin_protocol::bedrock::server::actor_event::{ActorEventID, SActorEvent};
                let event_id = if after.living.combat.health_milli == 0
                    && before.is_none_or(|previous| previous.living.combat.health_milli != 0)
                {
                    Some(ActorEventID::Death)
                } else if before.is_some_and(|previous| {
                    previous.living.combat.health_milli > after.living.combat.health_milli
                        || previous.living.combat.absorption_milli
                            > after.living.combat.absorption_milli
                }) {
                    Some(ActorEventID::Hurt)
                } else {
                    None
                };
                if let Some(event_id) = event_id {
                    frames.push(
                        client
                            .serialize_packet(&SActorEvent {
                                target_runtime_id: (id as u64).into(),
                                event_id,
                                data: 0.into(),
                                fire_at_position: None,
                            })
                            .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
    }
    frames.extend(
        super::cluster_living_metadata::prepare_player_sleep_transition(
            client,
            id,
            before.map(|frame| frame.living.as_ref()),
            &after.living,
        )?,
    );
    frames.extend(super::cluster_living_metadata::prepare_player_attributes(
        client,
        id,
        before
            .filter(|previous| {
                previous.mask_health == after.mask_health
                    && previous.health_policy == after.health_policy
            })
            .map(|frame| frame.living.as_ref()),
        &after.living,
        spatial.tick,
        after.mask_health,
    )?);
    if before.is_none_or(|previous| {
        !super::cluster_living_metadata::player_effects_presentation_equal(
            &previous.living,
            &after.living,
        )
    }) {
        frames.extend(super::cluster_living_metadata::prepare_player_effect_delta(
            client,
            id,
            before.map(|frame| frame.living.as_ref()),
            &after.living,
            spatial.tick,
        )?);
    }
    Ok(frames)
}

fn bedrock_metadata(
    frame: &RemotePlayerFrame,
) -> pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList {
    super::cluster_living_metadata::bedrock_player_presentation(&frame.living)
}

pub(super) fn prepare_break_animation(
    client: &crate::net::ClientPlatform,
    id: i32,
    animation: pumpkin_cluster::replica_visual::PlayerBreakAnimation,
) -> Result<Vec<bytes::Bytes>, String> {
    let [x, y, z] = animation.position;
    match client {
        crate::net::ClientPlatform::Java(client) => Ok(vec![
            client
                .serialize_packet(
                    &pumpkin_protocol::java::client::play::CSetBlockDestroyStage::new(
                        id.into(),
                        pumpkin_util::math::position::BlockPos::new(x, y, z),
                        animation.stage,
                    ),
                )
                .map_err(|error| error.to_string())?,
        ]),
        crate::net::ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::client::level_event::{CLevelEvent, LevelEvent};
            let (event, rate) = if animation.stage == -1 {
                (LevelEvent::BlockStopBreak, 0)
            } else if let Some(rate) = animation.bedrock_rate {
                (
                    if animation.start {
                        LevelEvent::BlockStartBreak
                    } else {
                        LevelEvent::BlockUpdateBreak
                    },
                    rate,
                )
            } else {
                return Ok(Vec::new());
            };
            Ok(vec![
                client
                    .serialize_packet(&CLevelEvent {
                        event_id: (event as i32).into(),
                        position: pumpkin_util::math::vector3::Vector3::new(
                            x as f32, y as f32, z as f32,
                        ),
                        data: rate.into(),
                    })
                    .map_err(|error| error.to_string())?,
            ])
        }
    }
}

pub(super) fn prepare_swing(
    client: &crate::net::ClientPlatform,
    id: i32,
    arm: pumpkin_cluster::replica_visual::SwingArm,
) -> Result<Vec<bytes::Bytes>, String> {
    match client {
        crate::net::ClientPlatform::Java(client) => {
            use pumpkin_protocol::java::client::play::Animation;
            if arm == pumpkin_cluster::replica_visual::SwingArm::Offhand
                && client.version.load() < pumpkin_util::version::JavaMinecraftVersion::V_1_9
            {
                return Ok(Vec::new());
            }
            let animation = match arm {
                pumpkin_cluster::replica_visual::SwingArm::Main => Animation::SwingMainArm,
                pumpkin_cluster::replica_visual::SwingArm::Offhand => Animation::SwingOffhand,
            };
            Ok(vec![
                crate::net::java::JavaClient::serialize_packet_for_version(
                    &pumpkin_protocol::java::client::play::CEntityAnimation::new(
                        id.into(),
                        animation,
                    ),
                    client.version.load(),
                )
                .map_err(|error| error.to_string())?,
            ])
        }
        crate::net::ClientPlatform::Bedrock(client) => Ok(vec![
            client
                .serialize_packet(&pumpkin_protocol::bedrock::server::animate::SAnimate {
                    action: pumpkin_protocol::bedrock::server::animate::AnimateAction::SwingArm,
                    target_actor_runtime_id: (id as u64).into(),
                    data: 0.0,
                    swing_source: None,
                })
                .map_err(|error| error.to_string())?,
        ]),
    }
}

pub(super) fn prepare_main_hand(
    client: &crate::net::ClientPlatform,
    id: i32,
    hand: u8,
) -> Result<Vec<bytes::Bytes>, String> {
    let crate::net::ClientPlatform::Java(client) = client else {
        return Ok(Vec::new());
    };
    let version = client.version.load();
    let mut metadata = Vec::new();
    for tracked in [
        pumpkin_data::tracked_data::player::PLAYER_MAIN_HAND,
        pumpkin_data::tracked_data::player::MAIN_ARM_ID,
    ] {
        if tracked.id.get(&version) != 255 {
            pumpkin_protocol::java::client::play::Metadata::new(tracked, hand)
                .write(&mut metadata, &version)
                .map_err(|error| error.to_string())?;
        }
    }
    if metadata.is_empty() {
        return Ok(Vec::new());
    }
    metadata.push(
        if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 {
            127
        } else {
            255
        },
    );
    Ok(vec![
        crate::net::java::JavaClient::serialize_packet_for_version(
            &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(
                id.into(),
                metadata.into_boxed_slice(),
            ),
            version,
        )
        .map_err(|error| error.to_string())?,
    ])
}

pub(super) fn prepare_skin(
    client: &crate::net::ClientPlatform,
    id: i32,
    mask: u8,
) -> Result<Vec<bytes::Bytes>, String> {
    let crate::net::ClientPlatform::Java(client) = client else {
        return Ok(Vec::new());
    };
    let version = client.version.load();
    let mut metadata = Vec::new();
    for tracked in [
        pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMISATION,
        pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMIZATION_ID,
    ] {
        if tracked.id.get(&version) != 255 {
            pumpkin_protocol::java::client::play::Metadata::new(tracked, mask)
                .write(&mut metadata, &version)
                .map_err(|error| error.to_string())?;
        }
    }
    metadata.push(
        if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 {
            127
        } else {
            255
        },
    );
    Ok(vec![
        crate::net::java::JavaClient::serialize_packet_for_version(
            &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(
                id.into(),
                metadata.into_boxed_slice(),
            ),
            version,
        )
        .map_err(|error| error.to_string())?,
    ])
}
