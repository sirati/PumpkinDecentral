use super::*;
use pumpkin_cluster::{
    actor::ActorId,
    canonical_action::{ActionId, SpatialEpoch},
    entity_body::{EntityPayload, EntitySpatial},
    identity::{GlobalPlayerId, PlayerSlot, ServerId},
    item_contact::ContactIdentity,
    protocol::{ChunkKey, WorldId},
    realm_completion::PeerGeneration,
    time::TickStamp,
};
use pumpkin_protocol::{MultiVersionJavaPacket, ser::NetworkReadExt};

fn vehicle(kind: &pumpkin_data::entity::EntityType) -> EntityBody {
    let owner = PeerGeneration {
        server: ServerId(1),
        generation: 2,
    };
    let id = GlobalEntityId {
        origin: owner.server,
        local_id: 12,
        generation: owner.generation,
    };
    let spatial = EntitySpatial {
        world: WorldId::new(0, 3),
        position: [8.0_f64, 80.0, 8.0].map(f64::to_bits),
        velocity_micro: [0; 3],
        yaw: 0.0_f32.to_bits(),
        pitch: 0.0_f32.to_bits(),
        on_ground: true,
    };
    super::super::cluster_vehicle_birth::prepare_body(
        super::super::cluster_vehicle_birth::VehicleBirthInput {
            entity: id,
            action: ActionId {
                actor: ActorId::Entity(id),
                incarnation: id.generation,
                sequence: 1,
                family: 1,
            },
            owner,
            tick: TickStamp(1),
            uuid: Uuid::from_u128(12),
            kind: kind.clone(),
            spatial,
        },
        ChunkKey::new(WorldId::new(0, 3), 0, 0),
    )
    .unwrap()
}

#[test]
fn vehicle_fuse_and_wobble_update_preserves_birth_and_receiver_identity() {
    let mut before = vehicle(&pumpkin_data::entity::EntityType::TNT_MINECART);
    let EntityPayload::Vehicle(state) = &mut before.payload else {
        unreachable!()
    };
    state.fuse_ticks = Some(20);
    let mut view = BodyView::default();
    let mut events = Vec::new();
    view.replace(before.id, Some(&before), None, &mut events);
    events.clear();
    let mut after = before.clone();
    let EntityPayload::Vehicle(state) = &mut after.payload else {
        unreachable!()
    };
    state.fuse_ticks = Some(19);
    state.wobble_ticks = 10;
    state.damage_milli = 1_000;
    view.replace(after.id, Some(&after), None, &mut events);
    assert!(
        matches!(events.as_slice(), [BodyPresentationEvent::Update { before, after }] if before.birth == after.birth)
    );
    let mut successor = after.clone();
    successor.birth.uuid = Uuid::from_u128(13);
    events.clear();
    view.replace(successor.id, Some(&successor), None, &mut events);
    assert!(matches!(
        events.as_slice(),
        [
            BodyPresentationEvent::Remove(_),
            BodyPresentationEvent::Spawn(_)
        ]
    ));
}

#[cfg(feature = "cluster-e2e")]
fn resolved_passengers(
    body: &EntityBody,
    views: &BTreeMap<
        (GlobalPlayerId, u64, u32),
        BTreeMap<
            GlobalPlayerId,
            super::super::cluster_remote_player_presentation::RemotePlayerFrame,
        >,
    >,
    viewer: (GlobalPlayerId, u64, u32),
    source: PeerGeneration,
) -> BTreeMap<GlobalEntityId, BTreeMap<u8, (ContactIdentity, i32)>> {
    BTreeMap::from([(
        body.id,
        body.passengers
            .iter()
            .filter_map(|(seat, identity)| {
                let ContactIdentity::Player(lease) = identity else {
                    return None;
                };
                super::super::cluster_remote_player_presentation::delivered_player_id(
                    views, viewer, *lease, source,
                )
                .map(|id| (*seat, (identity.clone(), id)))
            })
            .collect(),
    )])
}

#[cfg(feature = "cluster-e2e")]
fn passenger_ids(
    packets: &[bytes::Bytes],
    version: pumpkin_util::version::JavaMinecraftVersion,
) -> Vec<Vec<i32>> {
    packets
        .iter()
        .filter_map(|packet| {
            let mut packet = packet.as_ref();
            if packet.get_var_int().unwrap().0
                != pumpkin_protocol::java::client::play::CSetPassengers::to_id(version)
            {
                return None;
            }
            assert_eq!(packet.get_var_int().unwrap().0, 101);
            let count = packet.get_var_int().unwrap().0;
            let passengers = (0..count)
                .map(|_| packet.get_var_int().unwrap().0)
                .collect::<Vec<_>>();
            assert!(packet.is_empty());
            Some(passengers)
        })
        .collect()
}

#[cfg(feature = "cluster-e2e")]
#[tokio::test(flavor = "current_thread")]
async fn vehicle_links_follow_actual_player_birth_delivery_and_exact_lease_successor() {
    use super::super::{
        cluster_remote_player_packets as player_packets,
        cluster_remote_player_presentation as players,
    };
    let profile = crate::net::GameProfile {
        id: Uuid::from_u128(1),
        name: "viewer".into(),
        properties: arc_swap::ArcSwap::from_pointee(Vec::new()),
        profile_actions: None,
    };
    let (client, mut sink) = crate::net::java::JavaClient::offline(
        1,
        profile,
        crate::net::PlayerConfig::default(),
        pumpkin_data::packet::CURRENT_MC_VERSION,
        None,
    );
    let client = crate::net::ClientPlatform::Java(client);
    let version = client.java_version();
    let blocks = super::super::cluster_chunk_state::OwnedChunkStore::default();
    let frame_palette = super::super::cluster_object_frame::BedrockFramePalette::default();
    let player = players::baseline_test_frame(71);
    let viewer = (GlobalPlayerId::new(ServerId(1), PlayerSlot(1)), 10, 3);
    let mut delivered = BTreeMap::new();
    let mut boat = vehicle(&pumpkin_data::entity::EntityType::OAK_BOAT);
    boat.passengers
        .insert(0, ContactIdentity::Player(player.lease));
    let target = Target::Chunk {
        key: ChunkKey::new(boat.spatial.world, 0, 0),
        part: ChunkPart::Entity(boat.id),
        inventory: None,
    };
    let mut ids = OwnerEntityClientIds::new(Some(101), 10);
    let allocation = ids
        .prepare_bodies(std::iter::once((target, &boat)))
        .unwrap();
    ids.commit(allocation);
    let mut view = BodyView::default();
    let mut events = Vec::new();
    view.replace(boat.id, Some(&boat), None, &mut events);
    let previous = BodyView::default();
    view.refresh_passengers(
        &previous,
        resolved_passengers(&boat, &delivered, viewer, player.source),
        &mut events,
    );
    let resolve = |identity: &ContactIdentity| {
        let ContactIdentity::Player(lease) = identity else {
            return None;
        };
        players::delivered_player_id(&delivered, viewer, *lease, player.source)
    };
    let packets = prepare_body_events(
        &client,
        &ids,
        &events,
        &blocks,
        &frame_palette,
        None,
        &resolve,
    )
    .unwrap();
    assert_eq!(passenger_ids(&packets, version), vec![Vec::<i32>::new()]);
    let count = packets.len();
    for packet in packets {
        client.try_enqueue_packet_data(packet);
    }
    assert_eq!(sink.drain(4096).unwrap().packets, count);
    assert!(players::enqueue_viewer_projection(
        &mut delivered,
        viewer,
        &client,
        player_packets::prepare_frame(&client, None, &player).unwrap(),
        BTreeMap::from([(player.lease.gid, player.clone())])
    ));
    assert!(sink.drain(4096).unwrap().packets > 0);
    let previous = view.clone();
    events.clear();
    view.refresh_passengers(
        &previous,
        resolved_passengers(&boat, &delivered, viewer, player.source),
        &mut events,
    );
    let resolve = |identity: &ContactIdentity| {
        let ContactIdentity::Player(lease) = identity else {
            return None;
        };
        players::delivered_player_id(&delivered, viewer, *lease, player.source)
    };
    let packets = prepare_body_events(
        &client,
        &ids,
        &events,
        &blocks,
        &frame_palette,
        None,
        &resolve,
    )
    .unwrap();
    assert_eq!(passenger_ids(&packets, version), vec![vec![71]]);
    assert!(
        !packets
            .iter()
            .any(|packet| packet.as_ref().get_var_int().unwrap().0
                == pumpkin_protocol::java::client::play::CSpawnEntity::to_id(version))
    );
    let count = packets.len();
    for packet in packets {
        client.try_enqueue_packet_data(packet);
    }
    assert_eq!(sink.drain(4096).unwrap().packets, count);
    let mut successor = player.clone();
    successor.lease.incarnation += 1;
    successor.client_entity_id = 72;
    std::sync::Arc::make_mut(&mut successor.profile).session = 81;
    std::sync::Arc::make_mut(&mut successor.living)
        .avatar
        .as_mut()
        .unwrap()
        .spatial_epoch = SpatialEpoch::Admission {
        lease: successor.lease,
        admitted_at: TickStamp(2),
    };
    let mut packets = player_packets::prepare_remove(&client, 71).unwrap();
    packets.extend(player_packets::prepare_frame(&client, None, &successor).unwrap());
    assert!(players::enqueue_viewer_projection(
        &mut delivered,
        viewer,
        &client,
        packets,
        BTreeMap::from([(successor.lease.gid, successor.clone())])
    ));
    assert!(sink.drain(4096).unwrap().packets > 0);
    let previous = view.clone();
    events.clear();
    view.refresh_passengers(
        &previous,
        resolved_passengers(&boat, &delivered, viewer, successor.source),
        &mut events,
    );
    let resolve = |identity: &ContactIdentity| {
        let ContactIdentity::Player(lease) = identity else {
            return None;
        };
        players::delivered_player_id(&delivered, viewer, *lease, successor.source)
    };
    let packets = prepare_body_events(
        &client,
        &ids,
        &events,
        &blocks,
        &frame_palette,
        None,
        &resolve,
    )
    .unwrap();
    assert_eq!(passenger_ids(&packets, version), vec![Vec::<i32>::new()]);
    let count = packets.len();
    for packet in packets {
        client.try_enqueue_packet_data(packet);
    }
    assert_eq!(sink.drain(4096).unwrap().packets, count);
    boat.passengers
        .insert(0, ContactIdentity::Player(successor.lease));
    events.clear();
    view.replace(boat.id, Some(&boat), None, &mut events);
    let packets = prepare_body_events(
        &client,
        &ids,
        &events,
        &blocks,
        &frame_palette,
        None,
        &resolve,
    )
    .unwrap();
    assert_eq!(passenger_ids(&packets, version), vec![vec![72]]);
    let count = packets.len();
    for packet in packets {
        client.try_enqueue_packet_data(packet);
    }
    assert_eq!(sink.drain(4096).unwrap().packets, count);
}
