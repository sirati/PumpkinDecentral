use bytes::Bytes;
use rustc_hash::FxHashMap;

use super::cluster_remote_player_presentation::RemotePlayerFrame;
use pumpkin_cluster::canonical_action::{LivePlayerLease, SpatialEpoch};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::realm_completion::PeerGeneration;

#[derive(Clone, Copy, PartialEq, Eq)]
struct PacketKey {
    protocol: u16,
    before_image: Option<u64>,
    after_image: u64,
    before_mask: Option<bool>,
    after_mask: bool,
    before_policy: Option<(
        Option<PeerGeneration>,
        pumpkin_cluster::health_policy::HealthPolicySnapshot,
    )>,
    after_policy: (
        Option<PeerGeneration>,
        pumpkin_cluster::health_policy::HealthPolicySnapshot,
    ),
    lease: LivePlayerLease,
    source: PeerGeneration,
    epoch: Option<SpatialEpoch>,
    client_entity_id: i32,
}
struct CachedPackets {
    key: PacketKey,
    _before_image: Option<RemotePlayerFrame>,
    _after_image: RemotePlayerFrame,
    packets: Vec<Bytes>,
}

pub(super) struct PlayerPacketCache {
    next_image: u64,
    bytes: usize,
    packets: FxHashMap<GlobalPlayerId, Vec<CachedPackets>>,
    modified_images: FxHashMap<(u64, usize), u64>,
}
impl PlayerPacketCache {
    pub(super) fn new(next_image: u64) -> Self {
        Self {
            next_image,
            bytes: 0,
            packets: FxHashMap::default(),
            modified_images: FxHashMap::default(),
        }
    }
    pub(super) fn next_image(&self) -> u64 {
        self.next_image
    }
    pub(super) fn modified_image(&mut self, before: u64, change: usize) -> u64 {
        if let Some(image) = self.modified_images.get(&(before, change)) {
            return *image;
        }
        self.next_image = self.next_image.wrapping_add(1);
        if self.modified_images.len() < pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS {
            self.modified_images
                .insert((before, change), self.next_image);
        }
        self.next_image
    }
    pub(super) fn prepare(
        &mut self,
        client: &crate::net::ClientPlatform,
        before: Option<&RemotePlayerFrame>,
        after: &RemotePlayerFrame,
    ) -> Result<Vec<Bytes>, String> {
        let crate::net::ClientPlatform::Java(java) = client else {
            return super::cluster_remote_player_packets::prepare_frame(client, before, after);
        };
        let key = PacketKey {
            protocol: java.version.load() as u16,
            before_image: before.map(|frame| frame.packet_image),
            after_image: after.packet_image,
            before_mask: before.map(|frame| frame.mask_health),
            after_mask: after.mask_health,
            before_policy: before.map(|frame| frame.health_policy),
            after_policy: after.health_policy,
            lease: after.lease,
            source: after.source,
            epoch: after
                .living
                .avatar
                .as_ref()
                .map(|avatar| avatar.spatial_epoch),
            client_entity_id: after.client_entity_id,
        };
        if let Some(cached) = self
            .packets
            .get(&after.lease.gid)
            .and_then(|variants| variants.iter().find(|cached| cached.key == key))
        {
            return Ok(cached.packets.clone());
        }
        let packets = super::cluster_remote_player_packets::prepare_frame(client, before, after)?;
        let size = packets
            .iter()
            .map(Bytes::len)
            .sum::<usize>()
            .checked_add(std::mem::size_of::<CachedPackets>())
            .and_then(|bytes| bytes.checked_add(packets.len() * std::mem::size_of::<Bytes>()))
            .and_then(|bytes| bytes.checked_add(self.bytes));
        if let Some(bytes) = size
            .filter(|bytes| *bytes <= pumpkin_cluster::canonical_action::MAX_LOGICAL_ACTION_BYTES)
        {
            self.packets
                .entry(after.lease.gid)
                .or_default()
                .push(CachedPackets {
                    key,
                    _before_image: before.cloned(),
                    _after_image: after.clone(),
                    packets: packets.clone(),
                });
            self.bytes = bytes;
        }
        Ok(packets)
    }
}

#[cfg(all(test, feature = "cluster-e2e"))]
mod tests {
    use super::*;
    use pumpkin_protocol::MultiVersionJavaPacket;
    use pumpkin_protocol::ser::NetworkReadExt;

    fn client(id: u64) -> crate::net::ClientPlatform {
        let profile = crate::net::GameProfile {
            id: uuid::Uuid::from_u128(u128::from(id)),
            name: "viewer".into(),
            properties: arc_swap::ArcSwap::from_pointee(Vec::new()),
            profile_actions: None,
        };
        let (client, _sink) = crate::net::java::JavaClient::offline(
            id,
            profile,
            crate::net::PlayerConfig::default(),
            pumpkin_data::packet::CURRENT_MC_VERSION,
            None,
        );
        crate::net::ClientPlatform::Java(client)
    }
    #[tokio::test(flavor = "current_thread")]
    async fn identical_java_images_share_packet_storage_without_crossing_health_or_entity_identity()
    {
        let first = client(1);
        let second = client(2);
        let frame = super::super::cluster_remote_player_presentation::baseline_test_frame(71);
        let mut cache = PlayerPacketCache::new(100);
        let a = cache.prepare(&first, None, &frame).unwrap();
        let b = cache.prepare(&second, None, &frame).unwrap();
        assert_eq!(a, b);
        assert!(a.iter().zip(&b).all(|(a, b)| a.as_ptr() == b.as_ptr()));
        let mut unmasked = frame.clone();
        unmasked.mask_health = false;
        assert_ne!(cache.prepare(&first, None, &unmasked).unwrap(), a);
        let mut other_id = frame.clone();
        other_id.client_entity_id = 72;
        let packets = cache.prepare(&first, None, &other_id).unwrap();
        let version = pumpkin_data::packet::CURRENT_MC_VERSION;
        let spawn = packets
            .iter()
            .find_map(|packet| {
                let mut bytes = packet.as_ref();
                (bytes.get_var_int().unwrap().0
                    == pumpkin_protocol::java::client::play::CSpawnEntity::to_id(version))
                .then_some(bytes)
            })
            .unwrap();
        assert_eq!(
            pumpkin_protocol::java::client::play::CSpawnEntity::read_packet_data(spawn, &version)
                .unwrap()
                .entity_id
                .0,
            72
        );
        let mut policy_changed = frame.clone();
        policy_changed.health_policy.0 = Some(PeerGeneration {
            server: pumpkin_cluster::identity::ServerId(0),
            generation: 77,
        });
        policy_changed.health_policy.1.version += 1;
        let policy_packets = cache
            .prepare(&first, Some(&frame), &policy_changed)
            .unwrap();
        assert!(policy_packets.iter().any(|packet| {
            let mut bytes = packet.as_ref();
            bytes.get_var_int().unwrap().0
                == pumpkin_protocol::java::client::play::CSetEntityMetadata::to_id(version)
        }));
        assert_eq!(
            frame.living.combat.health_milli,
            policy_changed.living.combat.health_milli
        );
        let mut moved = frame.clone();
        moved.spatial.frame.effective.position[0] = 9.0;
        moved.packet_image = cache.modified_image(frame.packet_image, 0);
        let a = cache.prepare(&first, Some(&frame), &moved).unwrap();
        let b = cache.prepare(&second, Some(&frame), &moved).unwrap();
        assert!(a.iter().zip(&b).all(|(a, b)| a.as_ptr() == b.as_ptr()));
        let mut position = a[0].as_ref();
        assert_eq!(
            position.get_var_int().unwrap().0,
            pumpkin_protocol::java::client::play::CEntityPositionSync::to_id(version)
        );
        assert_eq!(position.get_var_int().unwrap().0, 71);
        assert_eq!(position.get_f64().unwrap(), 9.0);
    }
}
