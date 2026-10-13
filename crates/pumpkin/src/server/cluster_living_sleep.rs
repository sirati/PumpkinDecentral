use pumpkin_cluster::item_action::{ActorLivingState, PlayerSleepingState};
use pumpkin_protocol::java::client::play::{Animation, CEntityAnimation, CUseBed};
use pumpkin_util::{math::position::BlockPos, version::JavaMinecraftVersion};

#[derive(Clone, Copy)]
enum SleepTransition {
    None,
    Enter(PlayerSleepingState),
    Wake,
}
fn transition(
    before: Option<PlayerSleepingState>,
    after: Option<PlayerSleepingState>,
) -> SleepTransition {
    match (before, after) {
        (Some(_), None) => SleepTransition::Wake,
        (before, Some(after)) if before.map(|before| before.bed) != Some(after.bed) => {
            SleepTransition::Enter(after)
        }
        _ => SleepTransition::None,
    }
}
fn sleeping(living: &ActorLivingState) -> Option<PlayerSleepingState> {
    if living.combat.health_milli == 0 {
        return None;
    }
    living.avatar.as_ref()?.presentation.sleeping
}

pub(super) fn prepare_player_sleep_transition(
    client: &crate::net::ClientPlatform,
    entity_id: i32,
    before: Option<&ActorLivingState>,
    after: &ActorLivingState,
) -> Result<Vec<bytes::Bytes>, String> {
    let change = transition(before.and_then(sleeping), sleeping(after));
    match (client, change) {
        (crate::net::ClientPlatform::Java(client), SleepTransition::Wake) => client
            .serialize_packet(&CEntityAnimation::new(
                entity_id.into(),
                Animation::LeaveBed,
            ))
            .map(|packet| vec![packet])
            .map_err(|error| error.to_string()),
        (crate::net::ClientPlatform::Java(client), SleepTransition::Enter(sleep))
            if client.version.load() < JavaMinecraftVersion::V_1_14 =>
        {
            client
                .serialize_packet(&CUseBed::new(
                    entity_id.into(),
                    BlockPos::new(sleep.bed.x, sleep.bed.y, sleep.bed.z),
                ))
                .map(|packet| vec![packet])
                .map_err(|error| error.to_string())
        }
        (crate::net::ClientPlatform::Bedrock(client), SleepTransition::Wake) => client
            .serialize_packet(&wake_packet(entity_id))
            .map(|packet| vec![packet])
            .map_err(|error| error.to_string()),
        _ => Ok(Vec::new()),
    }
}
fn wake_packet(entity_id: i32) -> pumpkin_protocol::bedrock::server::animate::SAnimate {
    pumpkin_protocol::bedrock::server::animate::SAnimate {
        action: pumpkin_protocol::bedrock::server::animate::AnimateAction::WakeUp,
        target_actor_runtime_id: (entity_id as u64).into(),
        data: 0.0,
        swing_source: None,
    }
}
pub(super) fn publish_player_sleep_transition(
    client: &crate::net::ClientPlatform,
    entity_id: i32,
    before: Option<&ActorLivingState>,
    after: &ActorLivingState,
) -> bool {
    let Ok(packets) = prepare_player_sleep_transition(client, entity_id, before, after) else {
        return false;
    };
    for packet in packets {
        client.try_enqueue_packet_data(packet);
    }
    !client.closed()
}

#[cfg(test)]
mod replica_sleep_tests {
    use super::*;
    use pumpkin_protocol::serial::{PacketRead, PacketWrite};
    use pumpkin_protocol::{ClientPacket, ServerPacket};
    #[test]
    fn accepted_death_wakes_a_sleeping_avatar_and_suppresses_dead_baseline() {
        let mut living = super::super::cluster_actor_import::from_player_nbt(
            pumpkin_cluster::identity::GlobalPlayerId::new(
                pumpkin_cluster::identity::ServerId(2),
                pumpkin_cluster::identity::PlayerSlot(7),
            ),
            None,
        )
        .unwrap()
        .state;
        living.avatar.as_mut().unwrap().presentation.sleeping = Some(PlayerSleepingState {
            bed: pumpkin_cluster::protocol::BlockPos { x: -3, y: 64, z: 9 },
            since: pumpkin_cluster::time::TickStamp(20),
        });
        let before = sleeping(&living);
        assert!(before.is_some());
        living.combat.health_milli = 0;
        assert!(matches!(
            transition(before, sleeping(&living)),
            SleepTransition::Wake
        ));
        assert!(matches!(
            transition(None, sleeping(&living)),
            SleepTransition::None
        ));
        assert!(
            living
                .avatar
                .as_ref()
                .unwrap()
                .presentation
                .sleeping
                .is_some()
        );
    }
    #[test]
    fn sleep_enter_and_wake_preserve_java_legacy_and_bedrock_wire_events() {
        let sleep = PlayerSleepingState {
            bed: pumpkin_cluster::protocol::BlockPos { x: -3, y: 64, z: 9 },
            since: pumpkin_cluster::time::TickStamp(20),
        };
        let SleepTransition::Enter(enter) = transition(None, Some(sleep)) else {
            panic!()
        };
        for version in [JavaMinecraftVersion::V_1_8, JavaMinecraftVersion::V_1_13_2] {
            let packet = CUseBed::new(
                71.into(),
                BlockPos::new(enter.bed.x, enter.bed.y, enter.bed.z),
            );
            let mut bytes = Vec::new();
            packet.write_packet_data(&mut bytes, &version).unwrap();
            let decoded = CUseBed::read(&mut bytes.as_slice(), &version).unwrap();
            assert_eq!(decoded.entity_id.0, 71);
            assert_eq!(decoded.location, packet.location);
        }
        assert!(matches!(
            transition(Some(sleep), None),
            SleepTransition::Wake
        ));
        for version in [JavaMinecraftVersion::V_1_8, JavaMinecraftVersion::V_1_21_11] {
            let packet = CEntityAnimation::new(71.into(), Animation::LeaveBed);
            let mut bytes = Vec::new();
            packet.write_packet_data(&mut bytes, &version).unwrap();
            assert_eq!(bytes, vec![71, 2]);
        }
        let mut bytes = Vec::new();
        wake_packet(71).write(&mut bytes).unwrap();
        let decoded =
            pumpkin_protocol::bedrock::server::animate::SAnimate::read(&mut bytes.as_slice())
                .unwrap();
        assert_eq!(
            decoded.action,
            pumpkin_protocol::bedrock::server::animate::AnimateAction::WakeUp
        );
        assert_eq!(decoded.target_actor_runtime_id.0, 71);
        assert!(matches!(transition(None, None), SleepTransition::None));
        assert!(matches!(
            transition(Some(sleep), Some(sleep)),
            SleepTransition::None
        ));
    }
}
