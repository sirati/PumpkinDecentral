use super::*;
use pumpkin_cluster::{
    actor::ActorId,
    canonical_action::{ActionId, ActionRef, SpatialEpoch},
    entity_body::{EntityPayload, EntitySpatial},
    identity::ServerId,
    object_state::{ObjectKind, ObjectState, TntState},
    protocol::{GlobalEntityId, WorldId},
    realm_completion::PeerGeneration,
    time::TickStamp,
};

fn tnt() -> EntityBody {
    let owner = PeerGeneration {
        server: ServerId(1),
        generation: 1,
    };
    let id = GlobalEntityId {
        origin: owner.server,
        generation: owner.generation,
        local_id: 7,
    };
    let reference = ActionRef {
        id: ActionId {
            actor: ActorId::Entity(id),
            incarnation: 1,
            sequence: 0,
            family: 1,
        },
        tick: TickStamp(1),
    };
    EntityBody {
        id,
        birth: EntityBirth {
            action: reference,
            source: owner,
            uuid: Uuid::from_u128(7),
        },
        owner,
        kind: pumpkin_data::entity::EntityType::TNT.id,
        spatial: EntitySpatial {
            world: WorldId::new(0, 1),
            position: [0.5_f64, 64.0, 0.5].map(f64::to_bits),
            velocity_micro: [0; 3],
            yaw: 0.0_f32.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
        },
        spatial_epoch: SpatialEpoch::Action(reference),
        mount: None,
        passengers: Default::default(),
        relation_versions: Default::default(),
        tags: Default::default(),
        tags_revision: 1,
        payload: EntityPayload::Object(ObjectState {
            simulation_tick: Some(TickStamp(2)),
            property_versions: Default::default(),
            kind: ObjectKind::PrimedTnt(TntState {
                fuse: 10,
                power: 4.0_f32.to_bits(),
                block_state: pumpkin_data::Block::TNT.default_state.id.as_u16(),
                igniter: None,
            }),
        }),
    }
}

#[test]
fn fuse_update_preserves_existing_client_identity_instead_of_respawning() {
    let before = tnt();
    let mut view = BodyView::default();
    let mut events = Vec::new();
    view.replace(before.id, Some(&before), None, &mut events);
    events.clear();
    let mut after = before.clone();
    let EntityPayload::Object(object) = &mut after.payload else {
        unreachable!()
    };
    object.simulation_tick = Some(TickStamp(3));
    let ObjectKind::PrimedTnt(tnt) = &mut object.kind else {
        unreachable!()
    };
    tnt.fuse = 9;
    view.replace(after.id, Some(&after), None, &mut events);
    assert!(
        matches!(events.as_slice(), [BodyPresentationEvent::Update { before: old, after: new }] if old.birth == new.birth)
    );
}

#[test]
fn private_object_clock_changes_publish_truth_without_client_packets() {
    let before = tnt();
    let mut after = before.clone();
    let EntityPayload::Object(object) = &mut after.payload else {
        unreachable!()
    };
    object.simulation_tick = Some(TickStamp(3));
    object.property_versions.insert(
        pumpkin_cluster::object_state::ObjectProperty::SimulationTick,
        12,
    );
    let mut view = BodyView::default();
    let mut events = Vec::new();
    view.replace(before.id, Some(&before), None, &mut events);
    events.clear();
    view.replace(after.id, Some(&after), None, &mut events);
    assert!(events.is_empty());
    assert_eq!(view.bodies[&after.id], after);
}

#[tokio::test]
async fn bedrock_frame_projection_emits_block_and_block_actor_packets() {
    let mut body = tnt();
    body.kind = pumpkin_data::entity::EntityType::ITEM_FRAME.id;
    let frame = pumpkin_cluster::object_state::FrameState {
        attachment: pumpkin_cluster::protocol::BlockPos { x: 0, y: 64, z: 0 },
        facing: 2,
        glow: false,
        item: pumpkin_cluster::inventory::InventoryCell::empty(pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, 0)),
        rotation: 3,
        drop_chance: 1.0_f32.to_bits(),
        invisible: false,
        fixed: false,
    };
    body.payload = EntityPayload::Object(ObjectState { simulation_tick: None, property_versions: Default::default(), kind: ObjectKind::ItemFrame(frame.clone()) });
    let encoder = pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder::new();
    let palette = super::super::cluster_object_frame::BedrockFramePalette::default();
    let blocks = super::super::cluster_chunk_state::OwnedChunkStore::default();
    let packets = super::super::cluster_object_frame::project(&body, &frame, false, &blocks, &palette, &encoder).unwrap();
    let mut ids = Vec::new();
    for packet in packets {
        let mut decoder = pumpkin_protocol::bedrock::packet_decoder::BedrockBatchDecoder::new();
        let payload = decoder.get_packet_payload(packet.to_vec()).await.unwrap();
        let game = decoder.get_game_packet(&mut std::io::Cursor::new(payload)).unwrap();
        ids.push(game.id);
    }
    assert_eq!(ids, [21, 56]);
}

fn health_mob(kind: &pumpkin_data::entity::EntityType) -> EntityBody {
    let template = tnt();
    let identity = pumpkin_cluster::entity_origin::EntityOriginIdentity { id: template.id, uuid: template.birth.uuid, source: template.owner };
    let (mut body, _) = super::super::cluster_owned_mob_initial::prepare_initial_body(&identity, template.birth.action, template.owner, kind, template.spatial, &pumpkin_nbt::compound::NbtCompound::new()).unwrap();
    let EntityPayload::Living(living) = &mut body.payload else { unreachable!() };
    living.combat.health_milli = 6_000;
    body
}

#[tokio::test]
async fn bedrock_health_packets_change_numeric_values_when_policy_changes_without_health_change() {
    use pumpkin_protocol::serial::PacketRead;
    let body = health_mob(&pumpkin_data::entity::EntityType::COW);
    let encoder = pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder::new();
    for (mode, expected) in [
        (pumpkin_cluster::health_policy::HealthMaskMode::All, 20.0),
        (pumpkin_cluster::health_policy::HealthMaskMode::Off, 6.0),
        (pumpkin_cluster::health_policy::HealthMaskMode::Players, 6.0),
        (pumpkin_cluster::health_policy::HealthMaskMode::All, 20.0),
    ] {
        let context = super::super::cluster_entity_health::HealthRenderContext { mode, viewer: None };
        let packets = super::super::cluster_entity_health::prepare_bedrock(&body, 17, context, &encoder).unwrap();
        assert_eq!(packets.len(), 1);
        let mut decoder = pumpkin_protocol::bedrock::packet_decoder::BedrockBatchDecoder::new();
        let payload = decoder.get_packet_payload(packets[0].to_vec()).await.unwrap();
        let game = decoder.get_game_packet(&mut std::io::Cursor::new(payload)).unwrap();
        assert_eq!(game.id, 29);
        let mut input = std::io::Cursor::new(game.payload.as_ref());
        assert_eq!(pumpkin_protocol::codec::var_ulong::VarULong::read(&mut input).unwrap().0, 17);
        assert_eq!(pumpkin_protocol::codec::var_uint::VarUInt::read(&mut input).unwrap().0, 1);
        let _minimum = f32::read(&mut input).unwrap();
        let maximum = f32::read(&mut input).unwrap();
        let current = f32::read(&mut input).unwrap();
        assert_eq!(current, expected);
        if expected == 20.0 { assert_eq!(maximum, 20.0); }
    }
}

#[test]
fn health_visual_exemptions_and_exact_mount_lease_keep_actual_health() {
    use super::super::cluster_entity_health::{HealthRenderContext, values};
    let context = HealthRenderContext { mode: pumpkin_cluster::health_policy::HealthMaskMode::All, viewer: None };
    for kind in [&pumpkin_data::entity::EntityType::WOLF, &pumpkin_data::entity::EntityType::IRON_GOLEM] {
        assert_eq!(values(&health_mob(kind), context).unwrap().unwrap().0, 6.0);
    }
    let mut horse = health_mob(&pumpkin_data::entity::EntityType::HORSE);
    let lease = pumpkin_cluster::canonical_action::LivePlayerLease {
        key: pumpkin_cluster::canonical_action::PlayerKey(Uuid::from_u128(1)),
        gid: pumpkin_cluster::identity::GlobalPlayerId::new(ServerId(1), pumpkin_cluster::identity::PlayerSlot(1)),
        incarnation: 3,
    };
    horse.passengers.insert(0, pumpkin_cluster::item_contact::ContactIdentity::Player(lease));
    assert_eq!(values(&horse, HealthRenderContext { viewer: Some(lease), ..context }).unwrap().unwrap().0, 6.0);
    let successor = pumpkin_cluster::canonical_action::LivePlayerLease { incarnation: 4, ..lease };
    assert_eq!(values(&horse, HealthRenderContext { viewer: Some(successor), ..context }).unwrap().unwrap().0, 20.0);
}
