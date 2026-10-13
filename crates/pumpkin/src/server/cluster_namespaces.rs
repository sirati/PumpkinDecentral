use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::Server;

const MAX_RECORD_BYTES: usize = 1024;
const MAX_RECORDS: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceAdmission {
    pub peer: ServerId,
    pub incarnation: u64,
    pub certificate_fingerprint: [u8; 32],
    pub supersedes: Option<PeerGeneration>,
    pub role: pumpkin_cluster::membership::ClusterNodeRole,
    pub protocol_version: u16,
}

impl NamespaceAdmission {
    pub fn founding_proof(&self) -> pumpkin_cluster::chunk_genesis::FoundingAdmission {
        pumpkin_cluster::chunk_genesis::FoundingAdmission {
            certificate_fingerprint: self.certificate_fingerprint,
            role: self.role,
            protocol_version: self.protocol_version,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct NamespaceView {
    pub ready: bool,
    pub revision: u64,
    pub admitted: BTreeMap<ServerId, NamespaceAdmission>,
    pub spent: BTreeMap<PeerGeneration, [u8; 32]>,
    pub retained: BTreeMap<PeerGeneration, NamespaceAdmission>,
}

impl NamespaceView {
    pub fn authenticates_chunk_genesis(&self, genesis: &pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate) -> bool {
        self.ready && genesis.validate_structural().is_ok() && genesis.founding.iter().all(|(source, proof)| {
            self.retained.get(source).is_some_and(|admission| admission.peer == source.server
                && admission.incarnation == source.generation && admission.founding_proof() == *proof)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceError {
    InvalidLineage,
    ReusedGeneration,
    CertificateMismatch,
    Capacity,
    Io(String),
}

pub struct NamespaceRequest {
    admission: NamespaceAdmission,
}

pub struct NamespaceRuntime {
    durable: bool,
    tx: Option<mpsc::Sender<NamespaceRequest>>,
    view: Arc<ArcSwap<NamespaceView>>,
}

impl NamespaceRuntime {
    pub fn new(primary: bool) -> (Self, Option<mpsc::Receiver<NamespaceRequest>>) {
        let view = Arc::new(ArcSwap::from_pointee(NamespaceView::default()));
        let (tx, rx) = mpsc::channel(128);
        (Self { durable: primary, tx: Some(tx), view }, Some(rx))
    }

    pub fn snapshot(&self) -> Arc<NamespaceView> {
        self.view.load_full()
    }

    pub fn retained_collision(&self, admission: &NamespaceAdmission) -> Option<NamespaceError> {
        let view = self.view.load();
        let source = PeerGeneration {
            server: admission.peer,
            generation: admission.incarnation,
        };
        if view.spent.contains_key(&source) {
            return Some(NamespaceError::ReusedGeneration);
        }
        view.admitted
            .get(&admission.peer)
            .filter(|previous| {
                previous.incarnation == admission.incarnation
                    && (previous.certificate_fingerprint != admission.certificate_fingerprint
                        || previous.role != admission.role || previous.protocol_version != admission.protocol_version)
            })
            .map(|_| NamespaceError::CertificateMismatch)
    }

    pub fn try_observe(&self, admission: NamespaceAdmission) -> Result<(), NamespaceAdmission> {
        let Some(tx) = &self.tx else {
            return Err(admission);
        };
        tx.try_send(NamespaceRequest { admission })
            .map_err(|error| error.into_inner().admission)
    }
}

struct NamespaceJournal {
    file: File,
    view: NamespaceView,
    records: usize,
    poisoned: bool,
}

impl NamespaceJournal {
    fn transition(
        view: &mut NamespaceView,
        admission: &NamespaceAdmission,
    ) -> Result<bool, NamespaceError> {
        let source = PeerGeneration {
            server: admission.peer,
            generation: admission.incarnation,
        };
        if admission.protocol_version == 0 || admission.incarnation == 0 {
            return Err(NamespaceError::InvalidLineage);
        }
        if !view.retained.contains_key(&source) && view.retained.len() >= MAX_RECORDS {
            return Err(NamespaceError::Capacity);
        }
        if view.spent.contains_key(&source) {
            return Err(NamespaceError::ReusedGeneration);
        }
        if let Some(previous) = view.admitted.get(&admission.peer) {
            if previous.incarnation == admission.incarnation {
                if previous.certificate_fingerprint != admission.certificate_fingerprint {
                    return Err(NamespaceError::CertificateMismatch);
                }
                if previous != admission {
                    return Err(NamespaceError::InvalidLineage);
                }
                return Ok(false);
            }
            let old = PeerGeneration {
                server: previous.peer,
                generation: previous.incarnation,
            };
            if admission.supersedes != Some(old) {
                return Err(NamespaceError::InvalidLineage);
            }
            view.spent.insert(old, previous.certificate_fingerprint);
        } else if admission.supersedes.is_some() {
            return Err(NamespaceError::InvalidLineage);
        }
        view.retained.insert(source, admission.clone());
        view.admitted.insert(admission.peer, admission.clone());
        view.revision = view
            .revision
            .checked_add(1)
            .ok_or(NamespaceError::Capacity)?;
        Ok(true)
    }

    fn open(path: &Path) -> Result<Self, NamespaceError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(io_error)?;
        let mut view = NamespaceView::default();
        let mut good = 0u64;
        let mut records = 0usize;
        loop {
            let mut size = [0u8; 4];
            match file.read_exact(&mut size) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(io_error(error)),
            }
            let length = u32::from_le_bytes(size) as usize;
            if length == 0 || length > MAX_RECORD_BYTES || records >= MAX_RECORDS {
                return Err(NamespaceError::Capacity);
            }
            let mut checksum = [0u8; 32];
            let mut bytes = vec![0; length];
            if let Err(error) = file
                .read_exact(&mut checksum)
                .and_then(|()| file.read_exact(&mut bytes))
            {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(io_error(error));
            }
            if Sha256::digest(&bytes).as_slice() != checksum {
                return Err(NamespaceError::Io(
                    "namespace journal checksum mismatch".to_owned(),
                ));
            }
            let admission = postcard::from_bytes::<NamespaceAdmission>(&bytes)
                .map_err(|error| NamespaceError::Io(error.to_string()))?;
            if !Self::transition(&mut view, &admission)? {
                return Err(NamespaceError::InvalidLineage);
            }
            good = good
                .checked_add(36 + length as u64)
                .ok_or(NamespaceError::Capacity)?;
            records += 1;
        }
        if file.metadata().map_err(io_error)?.len() != good {
            file.set_len(good).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
        }
        file.seek(SeekFrom::End(0)).map_err(io_error)?;
        view.ready = true;
        Ok(Self {
            file,
            view,
            records,
            poisoned: false,
        })
    }

    fn admit(
        &mut self,
        mut admission: NamespaceAdmission,
    ) -> Result<NamespaceAdmission, NamespaceError> {
        if self.poisoned {
            return Err(NamespaceError::Io(
                "namespace observation writer is unavailable".to_owned(),
            ));
        }
        if admission.supersedes.is_none() {
            admission.supersedes = self
                .view
                .admitted
                .get(&admission.peer)
                .filter(|previous| previous.incarnation != admission.incarnation)
                .map(|previous| PeerGeneration {
                    server: previous.peer,
                    generation: previous.incarnation,
                });
        }
        let mut next = self.view.clone();
        if !Self::transition(&mut next, &admission)? {
            return Ok(admission);
        }
        if self.records >= MAX_RECORDS {
            return Err(NamespaceError::Capacity);
        }
        let bytes = postcard::to_allocvec(&admission)
            .map_err(|error| NamespaceError::Io(error.to_string()))?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(NamespaceError::Capacity);
        }
        let begin = self.file.stream_position().map_err(io_error)?;
        let written = self
            .file
            .write_all(&(bytes.len() as u32).to_le_bytes())
            .and_then(|()| self.file.write_all(&Sha256::digest(&bytes)))
            .and_then(|()| self.file.write_all(&bytes))
            .and_then(|()| self.file.sync_all());
        if let Err(error) = written {
            let rollback = self
                .file
                .set_len(begin)
                .and_then(|()| self.file.seek(SeekFrom::Start(begin)).map(|_| ()))
                .and_then(|()| self.file.sync_all());
            self.poisoned = rollback.is_err();
            return Err(NamespaceError::Io(format!(
                "{error}; rollback={rollback:?}"
            )));
        }
        self.view = next;
        self.records += 1;
        Ok(admission)
    }
}

fn io_error(error: std::io::Error) -> NamespaceError {
    NamespaceError::Io(error.to_string())
}

pub fn spawn(server: &Arc<Server>, requests: Option<mpsc::Receiver<NamespaceRequest>>) {
    let Some(mut requests) = requests else {
        return;
    };
    let view = Arc::clone(&server.cluster_namespaces.view);
    if !server.cluster_namespaces.durable {
        server.spawn_task(async move {
            let mut state = NamespaceView {ready:true,..NamespaceView::default()};
            view.store(Arc::new(state.clone()));
            let mut last_error = None::<std::time::Instant>;
            while let Some(request) = requests.recv().await {
                let mut admission = request.admission;
                if admission.supersedes.is_none() {
                    admission.supersedes = state.admitted.get(&admission.peer).filter(|previous| previous.incarnation != admission.incarnation)
                        .map(|previous| PeerGeneration {server:previous.peer,generation:previous.incarnation});
                }
                let mut candidate = state.clone();
                match NamespaceJournal::transition(&mut candidate,&admission) {
                    Ok(_) => { state = candidate; view.store(Arc::new(state.clone())); },
                    Err(reason) => {
                        let now = std::time::Instant::now();
                        if last_error.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                            last_error = Some(now);
                            tracing::error!(?reason,"cluster authenticated namespace observation rejected");
                        }
                    }
                }
            }
        });
        return;
    }
    server.spawn_task(async move {
        let mut journal = match tokio::task::spawn_blocking(|| {
            std::fs::create_dir_all("data").map_err(io_error)?;
            let journal =
                NamespaceJournal::open(Path::new("data/cluster-namespace-history.journal"))?;
            File::open("data")
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)?;
            Ok::<_, NamespaceError>(journal)
        })
        .await
        {
            Ok(Ok(journal)) => journal,
            result => {
                let error = match result {
                    Ok(Err(error)) => format!("{error:?}"),
                    Err(error) => error.to_string(),
                    Ok(Ok(_)) => unreachable!(),
                };
                tracing::error!(%error, "cluster namespace history journal unavailable");
                while requests.recv().await.is_some() {}
                return;
            }
        };
        view.store(Arc::new(journal.view.clone()));
        let mut last_error = None::<std::time::Instant>;
        while let Some(request) = requests.recv().await {
            let admission = request.admission;
            let written = tokio::task::spawn_blocking(move || {
                let result = journal.admit(admission);
                (journal, result)
            })
            .await;
            let (returned, result) = match written {
                Ok(result) => result,
                Err(error) => {
                    tracing::error!(%error, "cluster namespace history writer failed");
                    let mut retained = view.load().as_ref().clone();
                    retained.ready = false;
                    view.store(Arc::new(retained));
                    break;
                }
            };
            journal = returned;
            if result.is_ok() {
                view.store(Arc::new(journal.view.clone()));
            } else {
                let now = std::time::Instant::now();
                if last_error.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                    last_error = Some(now);
                    tracing::error!(?result, "cluster namespace history observation rejected");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_uses_retained_original_role_after_namespace_successor() {
        let original = PeerGeneration {server:ServerId(7),generation:11};
        let admission = NamespaceAdmission {peer:original.server,incarnation:original.generation,
            certificate_fingerprint:[7;32],supersedes:None,
            role:pumpkin_cluster::membership::ClusterNodeRole::Primary,protocol_version:1};
        let mut view = NamespaceView {ready:true,..NamespaceView::default()};
        assert_eq!(NamespaceJournal::transition(&mut view,&admission),Ok(true));
        let genesis = pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate {
            chunk:pumpkin_cluster::protocol::ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0,9),-1,2),
            lineage:[9;32],initial_image:[8;32],founding:BTreeMap::from([(original,admission.founding_proof())]),
            issuer:original,full_source_slot:42,
        };
        assert!(view.authenticates_chunk_genesis(&genesis));
        let successor = NamespaceAdmission {incarnation:12,supersedes:Some(original),
            role:pumpkin_cluster::membership::ClusterNodeRole::Secondary,..admission};
        assert_eq!(NamespaceJournal::transition(&mut view,&successor),Ok(true));
        assert!(view.authenticates_chunk_genesis(&genesis));
        let mut missing = view.clone();
        missing.retained.remove(&original);
        assert!(!missing.authenticates_chunk_genesis(&genesis));
        let mut tampered = genesis.clone();
        tampered.founding.get_mut(&original).unwrap().certificate_fingerprint = [6;32];
        assert!(!view.authenticates_chunk_genesis(&tampered));
    }
}
