use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pumpkin_protocol::bedrock::client::Skin;
use uuid::Uuid;

use super::Server;

const SKIN_MAX_BYTES: usize = 16 * 1_048_576;
const SKIN_FRAME_BYTES: usize = 800_000;
pub const PREFIX: &[u8; 8] = b"PSkin002";

pub fn native_skin(data: &pumpkin_protocol::bedrock::server::login::ClientData) -> Option<Skin> {
    use base64::Engine;
    use pumpkin_protocol::bedrock::client::player_list::{
        PersonaPiece, PieceTintColor, SkinAnimation,
    };
    let decode = |value: &str| {
        if value.len() > 1_398_104 {
            return None;
        }
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .ok()
            .filter(|bytes| bytes.len() <= 1_048_576)
    };
    let image = |width: i32, height: i32, value: &str| {
        let width = u32::try_from(width).ok()?;
        let height = u32::try_from(height).ok()?;
        if width > 512 || height > 512 {
            return None;
        }
        let bytes = decode(value)?;
        (width.checked_mul(height)?.checked_mul(4)? as usize == bytes.len())
            .then_some((width, height, bytes))
    };
    let (width, height, pixels) = image(
        data.skin_image_width,
        data.skin_image_height,
        &data.skin_data,
    )?;
    if width == 0 || height == 0 || data.skin_id.is_empty() || data.skin_id.len() > 1024 {
        return None;
    }
    let (cape_width, cape_height, cape_data) = image(
        data.cape_image_width,
        data.cape_image_height,
        &data.cape_data,
    )?;
    let patch = decode(&data.skin_resource_patch)?;
    let patch_json: serde_json::Value = serde_json::from_slice(&patch).ok()?;
    if !patch_json.get("geometry")?.is_object()
        || data.animated_image_data.len() > 64
        || data.persona_pieces.len() > 256
        || data.piece_tint_colours.len() > 256
    {
        return None;
    }
    let mut skin = Skin::steve();
    skin.skin_id = data.skin_id.clone();
    skin.full_id = data.skin_id.clone();
    skin.play_fab_id = data.play_fab_id.clone();
    skin.image_width = width;
    skin.image_height = height;
    skin.skin_data = pixels;
    skin.resource_patch = patch;
    skin.cape_width = cape_width;
    skin.cape_height = cape_height;
    skin.cape_data = cape_data;
    skin.cape_id = data.cape_id.clone();
    skin.geometry_data = decode(&data.skin_geometry)?;
    skin.geometry_data_engine_version = decode(&data.skin_geometry_version)?;
    skin.animation_data = decode(&data.skin_animation_data)?;
    skin.arm_size = data.arm_size.clone();
    skin.skin_color = data.skin_colour.clone();
    skin.is_persona = data.persona_skin;
    skin.is_premium = data.premium_skin;
    skin.is_trusted = data.trusted_skin;
    skin.override_appearance = data.override_skin;
    skin.persona_cape_on_classic = data.cape_on_classic_skin;
    skin.animations = data
        .animated_image_data
        .iter()
        .map(|animation| {
            let (width, height, bytes) = image(
                animation.image_width,
                animation.image_height,
                &animation.image,
            )?;
            if !animation.frames.is_finite()
                || animation.frames < 0.0
                || animation.frames > f32::MAX as f64
            {
                return None;
            }
            Some(SkinAnimation {
                image_width: width,
                image_height: height,
                image_data: bytes,
                frames: animation.frames as f32,
                animation_type: u32::try_from(animation.animation_type).ok()?,
                expression_type: u32::try_from(animation.animation_expression).ok()?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    const PIECES: &[(&str, &str)] = &[
        ("unknown", "persona_unknown"),
        ("skeleton", "persona_skeleton"),
        ("body", "persona_body"),
        ("skin", "persona_skin"),
        ("bottom", "persona_bottom"),
        ("feet", "persona_feet"),
        ("dress", "persona_dress"),
        ("top", "persona_top"),
        ("high_pants", "persona_high_pants"),
        ("hands", "persona_hand"),
        ("outerwear", "persona_outerwear"),
        ("facialhair", "persona_facial_hair"),
        ("mouth", "persona_mouth"),
        ("eyes", "persona_eyes"),
        ("hair", "persona_hair"),
        ("hood", "persona_hood"),
        ("back", "persona_back"),
        ("faceaccessory", "persona_face_accessory"),
        ("head", "persona_head"),
        ("legs", "persona_legs"),
        ("leftleg", "persona_left_leg"),
        ("rightleg", "persona_right_leg"),
        ("arms", "persona_arms"),
        ("leftarm", "persona_left_arm"),
        ("rightarm", "persona_right_arm"),
        ("capes", "persona_capes"),
        ("classicskin", "persona_classic_skin"),
        ("emote", "persona_emote"),
        ("unsupported", "unsupported"),
    ];
    skin.persona_pieces = data
        .persona_pieces
        .iter()
        .map(|piece| {
            Some(PersonaPiece {
                piece_id: piece.piece_id.clone(),
                piece_type: i32::try_from(PIECES.iter().position(|(name, alias)| {
                    piece.piece_type == *name || piece.piece_type == *alias
                })?)
                .ok()?,
                pack_id: Uuid::parse_str(&piece.pack_id).ok()?,
                is_default: piece.is_default,
                product_id: piece.product_id.clone(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    skin.piece_tint_colors = data
        .piece_tint_colours
        .iter()
        .map(|piece| {
            let colors = piece
                .colours
                .iter()
                .map(|color| {
                    u32::from_str_radix(color.trim_start_matches('#'), 16)
                        .ok()
                        .map(|value| value as i32)
                })
                .collect::<Option<Vec<_>>>()?;
            Some(PieceTintColor {
                piece_type: piece.piece_type.clone(),
                colors: colors.try_into().ok()?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    valid_skin(&skin).then_some(skin)
}

fn valid_skin(skin: &Skin) -> bool {
    let image = |width: u32, height: u32, data: &[u8], required: bool| {
        width <= 512
            && height <= 512
            && (!required || width != 0 && height != 0)
            && ((width == 0) == (height == 0))
            && width
                .checked_mul(height)
                .and_then(|pixels| pixels.checked_mul(4))
                .is_some_and(|size| data.len() == size as usize)
    };
    !skin.skin_id.is_empty()
        && skin.skin_id.len() <= 1024
        && image(skin.image_width, skin.image_height, &skin.skin_data, true)
        && image(skin.cape_width, skin.cape_height, &skin.cape_data, false)
        && skin.animations.len() <= 64
        && skin.persona_pieces.len() <= 256
        && skin.piece_tint_colors.len() <= 256
        && skin.animations.iter().all(|animation| {
            image(
                animation.image_width,
                animation.image_height,
                &animation.image_data,
                true,
            ) && animation.frames.is_finite()
                && animation.frames >= 0.0
        })
        && serde_json::from_slice::<serde_json::Value>(&skin.resource_patch)
            .ok()
            .is_some_and(|patch| {
                patch
                    .get("geometry")
                    .is_some_and(serde_json::Value::is_object)
            })
        && postcard::to_allocvec(skin).is_ok_and(|bytes| bytes.len() <= SKIN_MAX_BYTES - 1024)
}

#[derive(Clone)]
pub struct ProfileSkin {
    pub gid: pumpkin_cluster::identity::GlobalPlayerId,
    pub source: pumpkin_cluster::realm_completion::PeerGeneration,
    pub fingerprint: u64,
    pub session: u64,
    pub skin: Arc<Skin>,
}

pub struct SkinSeed {
    pub transport_epoch: Option<u64>,
    pub transport_seen: bool,
    pub session: u64,
    pub gid: pumpkin_cluster::identity::GlobalPlayerId,
    pub uuid: Uuid,
    pub skin: Skin,
    pub source: Option<pumpkin_cluster::realm_completion::PeerGeneration>,
    pub fingerprint: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SkinControl {
    session: u64,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    uuid: Uuid,
    fingerprint: u64,
    skin: Skin,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SkinFragment {
    #[serde(skip)]
    received_epoch: u64,
    #[serde(skip)]
    transport_seen: bool,
    session: u64,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    uuid: Uuid,
    fingerprint: u64,
    transfer: [u8; 16],
    index: u16,
    parts: u16,
    bytes: Vec<u8>,
}

struct SkinAssembly {
    received_epoch: u64,
    transport_seen: bool,
    session: u64,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    uuid: Uuid,
    fingerprint: u64,
    parts: Vec<Option<Vec<u8>>>,
    remaining: usize,
    transfer: [u8; 16],
}

impl SkinAssembly {
    fn new(fragment: &SkinFragment) -> Self {
        Self {
            received_epoch: fragment.received_epoch,
            transport_seen: fragment.transport_seen,
            session: fragment.session,
            source: fragment.source,
            gid: fragment.gid,
            uuid: fragment.uuid,
            fingerprint: fragment.fingerprint,
            parts: vec![None; usize::from(fragment.parts)],
            remaining: usize::from(fragment.parts),
            transfer: fragment.transfer,
        }
    }

    fn insert(&mut self, fragment: SkinFragment) -> Result<Option<SkinSeed>, ()> {
        if self.session != fragment.session
            || self.transfer != fragment.transfer
            || self.source != fragment.source
            || self.gid != fragment.gid
            || self.uuid != fragment.uuid
            || self.fingerprint != fragment.fingerprint
            || self.parts.len() != usize::from(fragment.parts)
        {
            return Err(());
        }
        if fragment.received_epoch > self.received_epoch {
            self.received_epoch = fragment.received_epoch;
            self.transport_seen = fragment.transport_seen;
        } else if fragment.received_epoch == self.received_epoch {
            self.transport_seen |= fragment.transport_seen;
        }
        let slot = self.parts.get_mut(usize::from(fragment.index)).ok_or(())?;
        if let Some(previous) = slot {
            return if *previous == fragment.bytes {
                Ok(None)
            } else {
                Err(())
            };
        }
        *slot = Some(fragment.bytes);
        self.remaining -= 1;
        if self.remaining != 0 {
            return Ok(None);
        }
        let size = self
            .parts
            .iter()
            .filter_map(Option::as_ref)
            .map(Vec::len)
            .sum::<usize>();
        if size > SKIN_MAX_BYTES {
            return Err(());
        }
        let mut bytes = Vec::with_capacity(size);
        for part in &self.parts {
            bytes.extend_from_slice(part.as_ref().ok_or(())?);
        }
        let control: SkinControl = postcard::from_bytes(&bytes).map_err(|_| ())?;
        if control.session != self.session
            || control.source != self.source
            || control.gid != self.gid
            || control.uuid != self.uuid
            || control.fingerprint != self.fingerprint
            || !valid_skin(&control.skin)
        {
            return Err(());
        }
        Ok(Some(SkinSeed {
            transport_epoch: Some(self.received_epoch),
            transport_seen: self.transport_seen,
            session: self.session,
            gid: self.gid,
            uuid: self.uuid,
            skin: control.skin,
            source: Some(self.source),
            fingerprint: Some(self.fingerprint),
        }))
    }
}

fn control_frames(control: &SkinControl) -> Option<Vec<Arc<[u8]>>> {
    let payload = postcard::to_allocvec(control).ok()?;
    if payload.len() > SKIN_MAX_BYTES {
        return None;
    }
    let parts = payload.len().div_ceil(SKIN_FRAME_BYTES);
    let transfer = *Uuid::new_v4().as_bytes();
    payload
        .chunks(SKIN_FRAME_BYTES)
        .enumerate()
        .map(|(index, bytes)| {
            let fragment = SkinFragment {
                received_epoch: 0,
                transport_seen: false,
                session: control.session,
                source: control.source,
                gid: control.gid,
                uuid: control.uuid,
                fingerprint: control.fingerprint,
                transfer,
                index: index as u16,
                parts: parts as u16,
                bytes: bytes.to_vec(),
            };
            let mut bytes = PREFIX.to_vec();
            bytes.extend_from_slice(&postcard::to_allocvec(&fragment).ok()?);
            Some(Arc::from(bytes))
        })
        .collect()
}

pub fn decode_control(
    server: &Server,
    parcel: &pumpkin_cluster::streams::InboundParcel,
) -> Option<SkinFragment> {
    let bytes = parcel.bytes.strip_prefix(PREFIX)?;
    if bytes.len() > 900_000 || parcel.generation == 0 {
        return None;
    }
    let transport = server
        .cluster_peer_generations
        .load()
        .get(&parcel.peer)
        .copied();
    if transport.is_some_and(|epoch| epoch > parcel.generation) {
        return None;
    }
    let mut fragment: SkinFragment = postcard::from_bytes(bytes).ok()?;
    fragment.received_epoch = parcel.generation;
    fragment.transport_seen = transport == Some(parcel.generation);
    if fragment.session == 0
        || fragment.source.server != parcel.peer
        || fragment.gid.server != parcel.peer
        || fragment.source.generation == 0
        || fragment.transport_seen
            && server
                .cluster_peer_incarnations
                .load()
                .get(&parcel.peer)
                .is_some_and(|generation| *generation != fragment.source.generation)
        || fragment.parts == 0
        || usize::from(fragment.parts) > SKIN_MAX_BYTES.div_ceil(SKIN_FRAME_BYTES)
        || fragment.index >= fragment.parts
        || fragment.bytes.is_empty()
        || fragment.bytes.len() > SKIN_FRAME_BYTES
    {
        return None;
    }
    Some(fragment)
}

pub fn seed(
    server: &Server,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    uuid: Uuid,
    skin: Skin,
    session: u64,
) -> Result<(), SkinSeed> {
    server
        .cluster_profile_skin_seeds
        .try_send(SkinSeed {
            transport_epoch: None,
            transport_seen: false,
            session,
            gid,
            uuid,
            skin,
            source: None,
            fingerprint: None,
        })
        .map_err(|error| error.into_inner())
}

pub(crate) fn fingerprint(properties: &[pumpkin_cluster::presence::PresenceProperty]) -> u64 {
    properties
        .iter()
        .filter(|property| property.name == "textures")
        .fold(0, |hash, property| {
            xxhash_rust::xxh64::xxh64(property.value.as_bytes(), hash)
        })
}

fn matches_profile_generation(
    skin: &ProfileSkin,
    profile: &pumpkin_cluster::presence::RemotePlayerEntry,
    generation: Option<u64>,
) -> bool {
    skin.gid == profile.gid
        && profile.source == Some(skin.source)
        && skin.session == profile.session
        && skin.source.server == profile.gid.server
        && generation == Some(skin.source.generation)
        && skin.fingerprint == fingerprint(&profile.properties)
}

pub(crate) fn matches_profile(
    server: &Server,
    skin: &ProfileSkin,
    profile: &pumpkin_cluster::presence::RemotePlayerEntry,
) -> bool {
    matches_profile_generation(
        skin,
        profile,
        server
            .cluster_peer_incarnations
            .load()
            .get(&profile.gid.server)
            .copied(),
    )
}

pub fn get(server: &Server, uuid: Uuid) -> Option<Arc<Skin>> {
    let presence = server.cluster_presence.snapshot();
    let profile = presence.by_uuid(uuid.as_bytes())?;
    let views = server.cluster_profile_skins.load();
    let skin = views.get(&uuid)?;
    matches_profile(server, skin, profile).then(|| Arc::clone(&skin.skin))
}

pub fn lobby_bedrock_player_list_entry(
    server: &Server,
    uuid: Uuid,
    name: &str,
    properties: &[pumpkin_cluster::presence::PresenceProperty],
) -> Option<pumpkin_protocol::bedrock::client::player_list::PlayerListEntry> {
    let views = server.cluster_profile_skins.load();
    let entry = views.get(&uuid)?;
    let presence = server.cluster_presence.snapshot();
    let profile = presence
        .entries
        .get(&entry.gid)
        .filter(|profile| profile.uuid == *uuid.as_bytes())?;
    if entry.fingerprint != fingerprint(properties) || !matches_profile(server, entry, profile) {
        return None;
    }
    Some(
        pumpkin_protocol::bedrock::client::player_list::PlayerListEntry {
            uuid,
            entity_unique_id: pumpkin_protocol::codec::var_long::VarLong(0),
            username: name.to_owned(),
            xuid: String::new(),
            platform_chat_id: String::new(),
            build_platform: pumpkin_protocol::bedrock::client::common::BuildPlatform::Unknown,
            skin: (*entry.skin).clone(),
            is_teacher: false,
            is_host: false,
            is_sub_client: false,
            player_color: [0; 4],
        },
    )
}

pub fn spawn(
    server: &Arc<Server>,
    mut seeds: tokio::sync::mpsc::Receiver<SkinSeed>,
    mut fragments: tokio::sync::mpsc::Receiver<SkinFragment>,
) {
    let server = Arc::downgrade(server);
    tokio::spawn(async move {
        let mut seen = HashSet::new();
        let mut fetch_retry = HashMap::new();
        let mut seeded = HashSet::new();
        let mut skins = HashMap::new();
        let mut changed = HashSet::new();
        let mut pending_seeds = HashMap::new();
        let mut assemblies = HashMap::new();
        let mut native_controls = HashMap::<
            Uuid,
            (
                (
                    pumpkin_cluster::identity::GlobalPlayerId,
                    Uuid,
                    u64,
                    u64,
                    u64,
                ),
                Vec<Arc<[u8]>>,
            ),
        >::new();
        let mut outgoing =
            HashMap::<pumpkin_cluster::identity::ServerId, HashMap<Uuid, usize>>::new();
        let mut peers = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                seed = seeds.recv(), if pending_seeds.len() < 128 => {
                    let Some(seed) = seed else { break; };
                    pending_seeds.insert((seed.gid, seed.uuid, seed.session, seed.source, seed.transport_epoch), seed);
                }
                fragment = fragments.recv(), if pending_seeds.len() < 128 => {
                    let Some(fragment) = fragment else { break; };
                    let Some(server) = server.upgrade() else { break; };
                    if server.cluster_presence.snapshot().retirement_sessions.get(&fragment.gid).is_some_and(|(source, session)| *source == fragment.source && fragment.session <= *session) { continue; }
                    let key = (fragment.source, fragment.gid, fragment.uuid, fragment.session);
                    if !assemblies.contains_key(&key) && assemblies.len() >= pumpkin_cluster::presence::PRESENCE_MAX_ENTRIES {
                        tracing::error!("cluster native skin assembly capacity exceeded"); continue;
                    }
                    let assembly = assemblies.entry(key).or_insert_with(|| SkinAssembly::new(&fragment));
                    match assembly.insert(fragment) {
                        Ok(Some(seed)) => { assemblies.remove(&key); pending_seeds.insert((seed.gid, seed.uuid, seed.session, seed.source, seed.transport_epoch), seed); }
                        Err(()) => { assemblies.remove(&key); tracing::warn!("invalid cluster native skin assembly"); }
                        Ok(None) => {},
                    }
                }
                _ = interval.tick() => {
                    let Some(server) = server.upgrade() else { break; };
                    let snapshot = server.cluster_presence.snapshot();
                    let live = server.cluster_peer_incarnations.load();
                    let local = pumpkin_cluster::identity::ServerId(server.advanced_config.cluster.server_id);
                    let completed = pending_seeds.iter().filter_map(|(key, seed)| {
                        let entry = snapshot.entries.get(&seed.gid).filter(|entry| entry.uuid == *seed.uuid.as_bytes() && entry.session == seed.session && entry.source.is_some_and(|source| live.get(&source.server) == Some(&source.generation) && seed.source.is_none_or(|seed_source| seed_source == source)));
                        (entry.is_some() && live.contains_key(&seed.gid.server) && seed.transport_epoch.is_none_or(|epoch|
                            server.cluster_peer_generations.load().get(&seed.gid.server) == Some(&epoch))).then_some(*key)
                    }).collect::<Vec<_>>();
                    for key in completed {
                        let Some(seed) = pending_seeds.remove(&key) else { continue; };
                        let Some(entry) = snapshot.entries.get(&seed.gid).filter(|entry| entry.uuid == *seed.uuid.as_bytes() && entry.session == seed.session && entry.source.is_some_and(|source| live.get(&source.server) == Some(&source.generation) && seed.source.is_none_or(|seed_source| seed_source == source))) else { tracing::warn!(uuid=%seed.uuid, "cluster skin profile is unavailable"); continue; };
                        let Some(generation) = live.get(&seed.gid.server).copied() else { continue; };
                        let key = (seed.gid, seed.uuid, fingerprint(&entry.properties), generation, entry.session);
                        if seed.source.is_some_and(|source| source.server != seed.gid.server || live.get(&source.server) != Some(&source.generation)) || seed.fingerprint.is_some_and(|value| value != key.2) { continue; }
                        if seed.source.is_none() {
                            if seed.gid.server != local { continue; }
                            let Some(generation) = live.get(&local) else { continue; };
                            let control = SkinControl { session: seed.session, source: pumpkin_cluster::realm_completion::PeerGeneration { server: local, generation: *generation }, gid: seed.gid, uuid: seed.uuid, fingerprint: key.2, skin: seed.skin.clone() };
                            let Some(frames) = control_frames(&control) else { tracing::warn!(uuid=%seed.uuid,"cluster native skin exceeds payload budget"); continue; };
                            native_controls.insert(seed.uuid, (key, frames));
                            for peer in server.cluster_connected_peers.load().iter() { outgoing.entry(*peer).or_default().insert(seed.uuid, 0); }
                        }
                        seen.insert(key); seeded.insert(key);
                        changed.insert(seed.uuid);
                        skins.insert(seed.uuid, ProfileSkin { gid: key.0, source: pumpkin_cluster::realm_completion::PeerGeneration { server: key.0.server, generation: key.3 }, fingerprint: key.2, session: key.4, skin: Arc::new(seed.skin) });
                    }
                    for peer in server.cluster_connected_peers.load().iter() {
                        let generation = server.cluster_peer_generations.load().get(peer).copied();
                        if peers.get(peer) != Some(&generation) { outgoing.entry(*peer).or_default().extend(native_controls.keys().copied().map(|uuid| (uuid, 0))); peers.insert(*peer, generation); }
                    }
                    peers.retain(|peer, _| server.cluster_connected_peers.load().contains(peer));
                    outgoing.retain(|peer, _| peers.contains_key(peer));
                    let mut sent = 0;
                    for (peer, uuids) in &mut outgoing {
                        let mut done = Vec::new();
                        for (uuid, index) in uuids.iter_mut() {
                            if sent >= 4 { break; }
                            let Some((_, frames)) = native_controls.get(uuid) else { done.push(*uuid); continue; };
                            let Some(bytes) = frames.get(*index) else { done.push(*uuid); continue; };
                            if server.cluster_global_outbound.try_event(&server, vec![*peer], bytes.to_vec()) {
                                *index += 1; sent += 1;
                                if *index == frames.len() { done.push(*uuid); }
                            } else { break; }
                        }
                        for uuid in done { uuids.remove(&uuid); }
                    }
                    assemblies.retain(|(source, gid, uuid, session), assembly: &mut SkinAssembly| {
                        let epoch = server.cluster_peer_generations.load().get(&source.server).copied();
                        assembly.transport_seen |= epoch == Some(assembly.received_epoch);
                        (epoch.is_none() && !assembly.transport_seen || epoch.is_some_and(|epoch| epoch <= assembly.received_epoch))
                            && (epoch != Some(assembly.received_epoch) || live.get(&source.server).is_none_or(|generation| *generation == source.generation))
                            && !snapshot.retirement_sessions.get(gid).is_some_and(|(retired_source, retired)| *source == *retired_source && session <= retired)
                            && snapshot.entries.get(gid).is_none_or(|entry| entry.source != Some(*source) || entry.session < *session || entry.uuid == *uuid.as_bytes() && entry.session == *session)
                    });
                    pending_seeds.retain(|(gid, _, session, _, _), seed| {
                        let transport = seed.transport_epoch.map(|expected| (expected, server.cluster_peer_generations.load().get(&gid.server).copied()));
                        if let Some((expected, actual)) = transport { seed.transport_seen |= actual == Some(expected); }
                        transport.is_none_or(|(expected, actual)| actual.is_none() && !seed.transport_seen || actual.is_some_and(|epoch| epoch <= expected))
                            && seed.source.is_none_or(|source| transport.is_some_and(|(expected, actual)| actual != Some(expected)) || live.get(&source.server).is_none_or(|generation| *generation == source.generation))
                            && !snapshot.retirement_sessions.get(gid).is_some_and(|(source, retired)| live.get(&gid.server) == Some(&source.generation) && seed.source.is_none_or(|seed_source| seed_source == *source) && session <= retired)
                            && snapshot.entries.get(gid).is_none_or(|entry| entry.source.is_none_or(|source| live.get(&gid.server) != Some(&source.generation) || seed.source.is_some_and(|seed_source| seed_source != source)) || entry.session <= *session)
                    });
                    let current = snapshot.entries.values().filter_map(|entry| live.get(&entry.gid.server).filter(|generation| entry.source == Some(pumpkin_cluster::realm_completion::PeerGeneration { server: entry.gid.server, generation: **generation })).map(|generation| (entry.gid, Uuid::from_bytes(entry.uuid), fingerprint(&entry.properties), *generation, entry.session))).collect::<HashSet<_>>();
                    seen.retain(|key| current.contains(key));
                    fetch_retry.retain(|key, _| current.contains(key));
                    seeded.retain(|key| current.contains(key));
                    let previous_skin_count = skins.len();
                    skins.retain(|uuid, entry: &mut ProfileSkin| current.contains(&(entry.gid, *uuid, entry.fingerprint, entry.source.generation, entry.session)));
                    native_controls.retain(|_, (key, _)| current.contains(key));
                    for entry in snapshot.entries.values() {
                        if tasks.len() >= 4 { break; }
                        let uuid = Uuid::from_bytes(entry.uuid);
                        let Some(generation) = live.get(&entry.gid.server).copied() else { continue; };
                        let key = (entry.gid, uuid, fingerprint(&entry.properties), generation, entry.session);
                        if entry.source != Some(pumpkin_cluster::realm_completion::PeerGeneration { server: entry.gid.server, generation }) { continue; }
                        if entry.native_skin || fetch_retry.get(&key).is_some_and(|retry| std::time::Instant::now() < *retry) || !seen.insert(key) { continue; }
                        let properties = entry.properties.iter().map(|property| pumpkin_protocol::Property { name: property.name.clone().into(), value: property.value.clone().into(), signature: property.signature.clone().map(Into::into) }).collect::<Vec<_>>();
                        tasks.spawn(async move {
                            let result = if properties.iter().any(|property| &*property.name == "textures") {
                                tokio::time::timeout(std::time::Duration::from_secs(5), crate::entity::player::Player::fetch_skin_async(&properties)).await.ok().flatten()
                            } else { Some(Skin::steve()) };
                            (key, result)
                        });
                    }
                    if !changed.is_empty() || skins.len() != previous_skin_count {
                        server.cluster_profile_skins.store(Arc::new(skins.clone()));
                    }
                    let pending = changed.iter().copied().collect::<Vec<_>>();
                    for uuid in pending {
                        if super::cluster_hide::refresh_profile_skin(&server, uuid) { changed.remove(&uuid); }
                    }
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Ok(((gid, uuid, fingerprint, generation, session), result))) = result {
                        if seeded.contains(&(gid, uuid, fingerprint, generation, session)) { continue; }
                        let Some(server) = server.upgrade() else { break; };
                        let snapshot = server.cluster_presence.snapshot();
                        if !snapshot.entries.get(&gid).is_some_and(|entry| entry.uuid == *uuid.as_bytes()
                            && entry.session == session && entry.source == Some(pumpkin_cluster::realm_completion::PeerGeneration { server: gid.server, generation }) && !entry.native_skin && self::fingerprint(&entry.properties) == fingerprint)
                            || server.cluster_peer_incarnations.load().get(&gid.server) != Some(&generation) { continue; }
                        if let Some(skin) = result {
                            changed.insert(uuid);
                            skins.insert(uuid, ProfileSkin { gid, source: pumpkin_cluster::realm_completion::PeerGeneration { server: gid.server, generation }, fingerprint, session, skin: Arc::new(skin) });
                        } else {
                            let key = (gid, uuid, fingerprint, generation, session);
                            seen.remove(&key);
                            fetch_retry.insert(key, std::time::Instant::now() + std::time::Duration::from_secs(30));
                            tracing::warn!(%uuid, fingerprint, "cluster profile skin could not be decoded");
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_skin_cache_does_not_cross_relogin_or_boot_identity() {
        use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(3));
        let mut profile = pumpkin_cluster::presence::RemotePlayerEntry::new(
            gid,
            [9; 16],
            "Alice".to_owned(),
            Vec::new(),
            1,
            true,
            true,
        );
        let skin = ProfileSkin {
            session: 1,
            gid,
            source: pumpkin_cluster::realm_completion::PeerGeneration {
                server: gid.server,
                generation: 11,
            },
            fingerprint: 0,
            skin: Arc::new(Skin::steve()),
        };
        profile.session = 1;
        profile.source = Some(skin.source);
        assert!(matches_profile_generation(&skin, &profile, Some(11)));
        assert!(!matches_profile_generation(&skin, &profile, Some(12)));
        profile.session = 2;
        assert!(!matches_profile_generation(&skin, &profile, Some(11)));
        profile.session = 1;
        profile.gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(4));
        assert!(!matches_profile_generation(&skin, &profile, Some(11)));
    }

    #[test]
    fn malformed_images_and_resource_patches_are_rejected_before_delivery() {
        let mut skin = Skin::steve();
        assert!(valid_skin(&skin));
        skin.skin_data.pop();
        assert!(!valid_skin(&skin));
        let mut skin = Skin::steve();
        skin.resource_patch = b"{}".to_vec();
        assert!(!valid_skin(&skin));
        let mut skin = Skin::steve();
        skin.cape_width = 1;
        assert!(!valid_skin(&skin));
    }

    #[test]
    fn total_skin_budget_includes_capes_and_animations() {
        let mut skin = Skin::steve();
        skin.cape_width = 512;
        skin.cape_height = 512;
        skin.cape_data = vec![0; 512 * 512 * 4];
        skin.animation_data = vec![0; SKIN_MAX_BYTES];
        assert!(!valid_skin(&skin));
    }
}

#[cfg(test)]
mod multipart_tests {
    use super::*;

    #[test]
    fn native_512_skin_reassembles_out_of_order_with_duplicate_frames() {
        use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
        let source = pumpkin_cluster::realm_completion::PeerGeneration {
            server: ServerId(2),
            generation: 9,
        };
        let mut skin = Skin::steve();
        skin.image_width = 512;
        skin.image_height = 512;
        skin.skin_data = vec![7; 512 * 512 * 4];
        assert!(valid_skin(&skin));
        let control = SkinControl {
            session: 1,
            source,
            gid: GlobalPlayerId::new(source.server, PlayerSlot(3)),
            uuid: Uuid::from_bytes([4; 16]),
            fingerprint: 0,
            skin,
        };
        let frames = control_frames(&control).unwrap();
        assert!(frames.len() > 1);
        assert!(frames.iter().all(|frame| frame.len() < 900_000));
        let mut fragments = frames
            .into_iter()
            .map(|bytes| {
                postcard::from_bytes::<SkinFragment>(bytes.strip_prefix(PREFIX).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        let mut assembly = SkinAssembly::new(&fragments[0]);
        let duplicate: SkinFragment =
            postcard::from_bytes(&postcard::to_allocvec(fragments.last().unwrap()).unwrap())
                .unwrap();
        assert!(assembly.insert(duplicate).unwrap().is_none());
        let mut result = None;
        while let Some(fragment) = fragments.pop() {
            result = assembly.insert(fragment).unwrap().or(result);
        }
        let seed = result.unwrap();
        assert_eq!(seed.gid, control.gid);
        assert_eq!(seed.source, Some(source));
        assert_eq!(seed.skin.skin_data, control.skin.skin_data);
        assert_eq!(seed.skin.resource_patch, control.skin.resource_patch);
    }
}
