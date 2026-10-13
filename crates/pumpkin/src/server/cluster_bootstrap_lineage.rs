use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use pumpkin_cluster::protocol::WorldId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BaselineFile {
    length: u64,
    digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct VanillaLineage {
    pub world: WorldId,
    pub issuer: PeerGeneration,
    pub full_source_slot: u64,
    files: BTreeMap<String, BaselineFile>,
    founding: BTreeMap<PeerGeneration, super::cluster_namespaces::NamespaceAdmission>,
}

fn invalid() -> io::Error {
    io::ErrorKind::InvalidData.into()
}

fn file_digest(path: &Path) -> io::Result<BaselineFile> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invalid());
    }
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        length = length.checked_add(count as u64).ok_or_else(invalid)?;
        hasher.update(&buffer[..count]);
    }
    if length != metadata.len() || file.metadata()?.len() != length {
        return Err(invalid());
    }
    Ok(BaselineFile {
        length,
        digest: hasher.finalize().into(),
    })
}

fn baseline(folder: &Path) -> io::Result<BTreeMap<String, BaselineFile>> {
    let mut files = BTreeMap::new();
    for directory in ["region", "entities"] {
        let path = folder.join(directory);
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !std::fs::symlink_metadata(&path)?.is_dir()
            || std::fs::symlink_metadata(&path)?.file_type().is_symlink()
        {
            return Err(invalid());
        }
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().into_string().map_err(|_| invalid())?;
            if !(name.ends_with(".mca") || name.ends_with(".mcc")) {
                return Err(invalid());
            }
            files.insert(format!("{directory}/{name}"), file_digest(&entry.path())?);
            if files.len() > 1_048_576 {
                return Err(invalid());
            }
        }
    }
    Ok(files)
}

fn encode(lineage: &VanillaLineage) -> io::Result<Vec<u8>> {
    let body = postcard::to_allocvec(lineage).map_err(|_| invalid())?;
    if body.len() > MAX_MANIFEST_BYTES {
        return Err(invalid());
    }
    let mut result = Vec::with_capacity(36 + body.len());
    result.extend_from_slice(b"CBL1");
    result.extend_from_slice(&Sha256::digest(&body));
    result.extend_from_slice(&body);
    Ok(result)
}

fn decode(bytes: &[u8]) -> io::Result<VanillaLineage> {
    if bytes.len() < 36
        || bytes.len() > MAX_MANIFEST_BYTES + 36
        || &bytes[..4] != b"CBL1"
        || Sha256::digest(&bytes[36..]).as_slice() != &bytes[4..36]
    {
        return Err(invalid());
    }
    postcard::from_bytes(&bytes[36..]).map_err(|_| invalid())
}

impl VanillaLineage {
    pub fn open_or_create(
        folder: &Path,
        world: WorldId,
        issuer: PeerGeneration,
        full_source_slot: u64,
        founding: BTreeMap<PeerGeneration, super::cluster_namespaces::NamespaceAdmission>,
    ) -> io::Result<Self> {
        if issuer.generation == 0 || founding.is_empty()
            || founding.iter().any(|(source, admission)| source.server != admission.peer
                || source.generation != admission.incarnation || source.generation == 0 || admission.protocol_version == 0)
            || founding.get(&issuer).is_none_or(|admission| admission.role != pumpkin_cluster::membership::ClusterNodeRole::Primary) {
            return Err(invalid());
        }
        let directory = folder.join("cluster-bootstrap");
        let path = directory.join("lineage.cbl");
        match std::fs::File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take((MAX_MANIFEST_BYTES + 37) as u64).read_to_end(&mut bytes)?;
                let lineage = decode(&bytes)?;
                if lineage.world != world || lineage.files != baseline(folder)? {
                    return Err(invalid());
                }
                return Ok(lineage);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        for evidence in [folder.join("cluster-checkpoints"), directory.clone()] {
            match std::fs::read_dir(evidence) {
                Ok(mut entries) => {
                    if entries.next().transpose()?.is_some() {
                        return Err(invalid());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let lineage = Self {
            world,
            issuer,
            full_source_slot,
            files: baseline(folder)?,
            founding,
        };
        std::fs::create_dir_all(&directory)?;
        let temporary = directory.join("lineage.pending");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&encode(&lineage)?)?;
        file.sync_all()?;
        if lineage.files != baseline(folder)? {
            return Err(invalid());
        }
        std::fs::hard_link(&temporary, &path)?;
        std::fs::File::open(&directory)?.sync_all()?;
        std::fs::remove_file(temporary)?;
        std::fs::File::open(&directory)?.sync_all()?;
        Ok(lineage)
    }

    pub fn validate_baseline(&self, folder: &Path) -> io::Result<()> {
        if self.files != baseline(folder)? {
            return Err(invalid());
        }
        Ok(())
    }

    fn region_document(&self, folder: &Path, directory: &str, x: i32, z: i32) -> io::Result<Option<pumpkin_nbt::NbtCompound>> {
        let relative = format!("{directory}/r.{}.{}.mca", x.div_euclid(32), z.div_euclid(32));
        let Some(expected) = self.files.get(&relative) else {
            if folder.join(&relative).try_exists()? { return Err(invalid()); }
            return Ok(None);
        };
        let path = folder.join(&relative);
        if &file_digest(&path)? != expected || expected.length < 8192 { return Err(invalid()); }
        let mut file = std::fs::File::open(&path)?;
        let index = x.rem_euclid(32) as u64 + z.rem_euclid(32) as u64 * 32;
        file.seek(SeekFrom::Start(index * 4))?;
        let mut entry = [0_u8; 4];
        file.read_exact(&mut entry)?;
        let sector = u64::from(u32::from_be_bytes([0, entry[0], entry[1], entry[2]]));
        let count = u64::from(entry[3]);
        if sector == 0 && count == 0 { return Ok(None); }
        if sector < 2 || count == 0 || (sector + count) * 4096 > expected.length { return Err(invalid()); }
        file.seek(SeekFrom::Start(sector * 4096))?;
        let mut header = [0_u8; 5];
        file.read_exact(&mut header)?;
        let length = u32::from_be_bytes(header[..4].try_into().map_err(|_| invalid())?) as usize;
        if length == 0 || length as u64 + 4 > count * 4096 { return Err(invalid()); }
        let mut compressed = Vec::new();
        if header[4] & 128 != 0 {
            if length != 1 { return Err(invalid()); }
            let external = format!("{directory}/c.{x}.{z}.mcc");
            let expected = self.files.get(&external).ok_or_else(invalid)?;
            let external = folder.join(external);
            if &file_digest(&external)? != expected || expected.length > MAX_MANIFEST_BYTES as u64 { return Err(invalid()); }
            compressed = std::fs::read(external)?;
        } else {
            compressed.resize(length - 1, 0);
            file.read_exact(&mut compressed)?;
        }
        let mut bytes = Vec::new();
        let reader: Box<dyn Read + '_> = match header[4] & 127 {
            1 => Box::new(flate2::read::GzDecoder::new(compressed.as_slice())),
            2 => Box::new(flate2::read::ZlibDecoder::new(compressed.as_slice())),
            3 => Box::new(compressed.as_slice()),
            _ => return Err(invalid()),
        };
        reader.take(MAX_MANIFEST_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > MAX_MANIFEST_BYTES { return Err(invalid()); }
        let mut cursor = io::Cursor::new(bytes.as_slice());
        let document = {
            let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(
                pumpkin_nbt::deserializer::NbtStreamReader(&mut cursor));
            pumpkin_nbt::Nbt::read(&mut reader).map_err(|_| invalid())?
        };
        if cursor.position() as usize != bytes.len() { return Err(invalid()); }
        Ok(Some(document.root_tag))
    }

    pub fn entity_records(&self, folder: &Path, x: i32, z: i32) -> io::Result<Vec<pumpkin_nbt::NbtCompound>> {
        use pumpkin_nbt::tag::NbtTag;
        let mut records = Vec::new();
        if let Some(document) = self.region_document(folder,"entities",x,z)? {
            if !matches!(document.get("Position"),Some(NbtTag::IntArray(position)) if position.as_slice() == [x,z]) { return Err(invalid()); }
            let Some(NbtTag::List(entities)) = document.get("Entities") else { return Err(invalid()); };
            for entity in entities {
                let NbtTag::Compound(record) = entity else { return Err(invalid()); };
                records.push(record.clone());
            }
        }
        if let Some(document) = self.region_document(folder,"region",x,z)? {
            let root = match document.get("Level") {
                Some(NbtTag::Compound(level)) => level,
                Some(_) => return Err(invalid()),
                None => &document,
            };
            if !matches!(root.get("xPos"),Some(NbtTag::Int(position)) if *position == x)
                || !matches!(root.get("zPos"),Some(NbtTag::Int(position)) if *position == z) { return Err(invalid()); }
            match root.get("Entities") {
                Some(NbtTag::List(entities)) => {
                    for entity in entities {
                        let NbtTag::Compound(record) = entity else { return Err(invalid()); };
                        records.push(record.clone());
                    }
                }
                Some(_) => return Err(invalid()),
                None => {},
            }
        }
        Ok(records)
    }

    pub fn baseline_digest(&self) -> io::Result<[u8; 32]> {
        let encoded = postcard::to_allocvec(&(self.world, &self.files)).map_err(|_| invalid())?;
        let mut digest = Sha256::new();
        digest.update(b"pumpkin-vanilla-world-baseline-v1");
        digest.update(encoded);
        Ok(digest.finalize().into())
    }

    pub fn genesis_primary(&self, chunk: pumpkin_cluster::protocol::ChunkKey) -> io::Result<PeerGeneration> {
        if chunk.world != self.world { return Err(invalid()); }
        let founding = self.founding.iter().map(|(source,admission)| (*source,admission.founding_proof())).collect();
        pumpkin_cluster::chunk_genesis::elected_primary(chunk,self.baseline_digest()?,&founding).map_err(|_| invalid())
    }

    pub fn genesis_certificate(&self, chunk: pumpkin_cluster::protocol::ChunkKey, initial_image: [u8;32], full_source_slot: u64) -> io::Result<pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate> {
        let genesis = pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate {
            chunk, lineage:self.baseline_digest()?,initial_image,
            founding:self.founding.iter().map(|(source,admission)| (*source,admission.founding_proof())).collect(),
            issuer:self.genesis_primary(chunk)?,full_source_slot,
        };
        genesis.validate_structural().map_err(|_| invalid())?;
        Ok(genesis)
    }

    pub fn digest(&self) -> io::Result<[u8; 32]> {
        Ok(Sha256::digest(encode(self)?).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::ServerId;

    fn folder() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("pumpkin-lineage-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("region")).unwrap();
        std::fs::write(path.join("region/r.0.0.mca"), b"original region").unwrap();
        path
    }

    fn issuer() -> PeerGeneration {
        PeerGeneration { server: ServerId(1), generation: 7 }
    }

    fn founding() -> BTreeMap<PeerGeneration, super::super::cluster_namespaces::NamespaceAdmission> {
        BTreeMap::from([(issuer(), super::super::cluster_namespaces::NamespaceAdmission {
            peer: issuer().server, incarnation: issuer().generation, certificate_fingerprint: [9; 32],
            supersedes: None, role: pumpkin_cluster::membership::ClusterNodeRole::Primary,
            protocol_version: pumpkin_cluster::membership::CLUSTER_PROTOCOL_VERSION,
        })])
    }

    #[test]
    fn durable_lineage_detects_added_entities_and_changed_regions() {
        let path = folder();
        let lineage = VanillaLineage::open_or_create(&path, WorldId::new(0, 9), issuer(), 42, founding()).unwrap();
        let reopened = VanillaLineage::open_or_create(&path, WorldId::new(0, 9), issuer(), 43, founding()).unwrap();
        assert_eq!(lineage, reopened);
        assert_eq!(lineage.digest().unwrap(), reopened.digest().unwrap());
        std::fs::create_dir(path.join("entities")).unwrap();
        std::fs::write(path.join("entities/r.0.0.mca"), b"new entity").unwrap();
        assert!(lineage.validate_baseline(&path).is_err());
        std::fs::remove_file(path.join("entities/r.0.0.mca")).unwrap();
        std::fs::write(path.join("region/r.0.0.mca"), b"modified region").unwrap();
        assert!(lineage.validate_baseline(&path).is_err());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn founding_election_does_not_depend_on_creating_primary_or_phase() {
        let first = folder();
        let second = folder();
        let mut founding = founding();
        let other = PeerGeneration { server: ServerId(2), generation: 8 };
        let mut certificate = founding.get(&issuer()).unwrap().clone();
        certificate.peer = other.server;
        certificate.incarnation = other.generation;
        certificate.certificate_fingerprint = [8; 32];
        founding.insert(other, certificate);
        let world = WorldId::new(0, 9);
        let one = VanillaLineage::open_or_create(&first, world, issuer(), 42, founding.clone()).unwrap();
        let two = VanillaLineage::open_or_create(&second, world, other, 43, founding).unwrap();
        for x in -64..64 {
            let chunk = pumpkin_cluster::protocol::ChunkKey::new(world, x, 3);
            assert_eq!(one.genesis_primary(chunk).unwrap(), two.genesis_primary(chunk).unwrap());
        }
        std::fs::remove_dir_all(first).unwrap();
        std::fs::remove_dir_all(second).unwrap();
    }

    #[test]
    fn absent_lineage_cannot_adopt_existing_cluster_evidence() {
        let path = folder();
        std::fs::create_dir(path.join("cluster-checkpoints")).unwrap();
        std::fs::write(path.join("cluster-checkpoints/chunk.cpk"), b"existing truth").unwrap();
        assert!(VanillaLineage::open_or_create(&path, WorldId::new(0, 9), issuer(), 42, founding()).is_err());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn complete_entity_domain_rejects_malformed_records() {
        for malformed in [false, true] {
            let path = folder();
            std::fs::create_dir(path.join("entities")).unwrap();
            let mut root = pumpkin_nbt::NbtCompound::new();
            root.put("Position", pumpkin_nbt::tag::NbtTag::IntArray(vec![-1, 2]));
            let mut entity = pumpkin_nbt::NbtCompound::new();
            entity.put_string("id", "minecraft:item".into());
            let records = if malformed { vec![pumpkin_nbt::tag::NbtTag::Int(1)] }
                else { vec![pumpkin_nbt::tag::NbtTag::Compound(entity.clone())] };
            root.put_list("Entities", records);
            let payload = pumpkin_nbt::Nbt::new(String::new(), root).write();
            let sectors = (payload.len() + 5).div_ceil(4096);
            let mut region = vec![0; (2 + sectors) * 4096];
            let index = 31 + 2 * 32;
            region[index * 4..index * 4 + 4].copy_from_slice(&[0, 0, 2, sectors as u8]);
            region[8192..8196].copy_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
            region[8196] = 3;
            region[8197..8197 + payload.len()].copy_from_slice(&payload);
            std::fs::write(path.join("entities/r.-1.0.mca"), region).unwrap();
            let lineage = VanillaLineage::open_or_create(&path, WorldId::new(0, 9), issuer(), 42, founding()).unwrap();
            let decoded = lineage.entity_records(&path, -1, 2);
            if malformed { assert!(decoded.is_err()); } else { assert_eq!(decoded.unwrap(), vec![entity]); }
            assert!(lineage.entity_records(&path, 100, 100).unwrap().is_empty());
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn embedded_legacy_entity_domain_is_not_inferred_empty() {
        let path = folder();
        let mut entity = pumpkin_nbt::NbtCompound::new();
        entity.put_string("id","minecraft:armor_stand".into());
        let mut level = pumpkin_nbt::NbtCompound::new();
        level.put("xPos",pumpkin_nbt::tag::NbtTag::Int(0));
        level.put("zPos",pumpkin_nbt::tag::NbtTag::Int(0));
        level.put_list("Entities",vec![pumpkin_nbt::tag::NbtTag::Compound(entity.clone())]);
        let mut root = pumpkin_nbt::NbtCompound::new();
        root.put("Level",pumpkin_nbt::tag::NbtTag::Compound(level));
        let payload = pumpkin_nbt::Nbt::new(String::new(),root).write();
        let sectors = (payload.len()+5).div_ceil(4096);
        let mut region = vec![0;(2+sectors)*4096];
        region[..4].copy_from_slice(&[0,0,2,sectors as u8]);
        region[8192..8196].copy_from_slice(&((payload.len()+1) as u32).to_be_bytes());
        region[8196] = 3;
        region[8197..8197+payload.len()].copy_from_slice(&payload);
        std::fs::write(path.join("region/r.0.0.mca"),region).unwrap();
        let lineage = VanillaLineage::open_or_create(&path,WorldId::new(0,9),issuer(),42,founding()).unwrap();
        assert_eq!(lineage.entity_records(&path,0,0).unwrap(),vec![entity]);
        std::fs::create_dir(path.join("entities")).unwrap();
        std::fs::write(path.join("entities/r.0.0.mca"),[0;8192]).unwrap();
        assert!(lineage.entity_records(&path,0,0).is_err());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn corrupted_manifest_and_symlinked_domain_are_rejected() {
        let path = folder();
        let lineage = VanillaLineage::open_or_create(&path, WorldId::new(0, 9), issuer(), 42, founding()).unwrap();
        let mut encoded = encode(&lineage).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 1;
        assert!(decode(&encoded).is_err());
        std::os::unix::fs::symlink(path.join("region/r.0.0.mca"), path.join("region/r.1.0.mca")).unwrap();
        assert!(lineage.validate_baseline(&path).is_err());
        std::fs::remove_dir_all(path).unwrap();
    }
}
