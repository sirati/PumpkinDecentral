use std::collections::{HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use pumpkin_cluster::admin_sync::{
    AdminAudit, AdminControlMessage, decode_control_message, encode_control_message,
};
use tokio::sync::mpsc;

use super::Server;

const MAX_RECORD: usize = 65_536;

pub(crate) struct AcceptedMutationIds {
    ids: HashSet<[u8; 16]>,
    order: VecDeque<[u8; 16]>,
}

impl AcceptedMutationIds {
    pub(crate) fn from_replay(ids: Vec<[u8; 16]>) -> Self {
        let mut accepted = Self {
            ids: HashSet::new(),
            order: VecDeque::new(),
        };
        for id in ids
            .into_iter()
            .rev()
            .take(4096)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            accepted.insert(id);
        }
        accepted
    }

    pub(crate) fn contains(&self, id: &[u8; 16]) -> bool {
        self.ids.contains(id)
    }

    pub(crate) fn insert(&mut self, id: [u8; 16]) {
        if !self.ids.insert(id) {
            return;
        }
        self.order.push_back(id);
        if self.order.len() > 4096 {
            if let Some(old) = self.order.pop_front() {
                self.ids.remove(&old);
            }
        }
    }
}

fn path() -> std::io::Result<PathBuf> {
    let root = std::env::current_dir()?;
    let directory = root.join("data");
    let created = !directory.exists();
    std::fs::create_dir_all(&directory)?;
    if created {
        File::open(root)?.sync_all()?;
    }
    Ok(directory.join("cluster-admin.journal"))
}

pub(crate) fn replay(server: &Server) -> std::io::Result<Vec<[u8; 16]>> {
    let records = read_records(&path()?)?;
    let mut applied = Vec::with_capacity(records.len());
    for (id, audit) in records {
        super::cluster_admin_apply::apply_replayed_mutation_to_server(server, &audit.mutation);
        applied.push(id);
    }
    Ok(applied)
}

fn read_records(path: &std::path::Path) -> std::io::Result<Vec<([u8; 16], AdminAudit)>> {
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut records = Vec::new();
    let mut offset = 0;
    loop {
        let mut length = [0_u8; 4];
        match file.read_exact(&mut length) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(offset)?;
                file.sync_all()?;
                return Ok(records);
            }
            Err(error) => return Err(error),
        }
        let length = u32::from_le_bytes(length) as usize;
        if length == 0 || length > MAX_RECORD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid admin journal record length",
            ));
        }
        let mut bytes = vec![0; length];
        match file.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(offset)?;
                file.sync_all()?;
                return Ok(records);
            }
            Err(error) => return Err(error),
        }
        let AdminControlMessage::SequencedMutation { id, audit } =
            decode_control_message(&bytes)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
        else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected admin journal record",
            ));
        };
        records.push((id, audit));
        offset += 4 + length as u64;
    }
}

pub(crate) fn spawn(
    requests: mpsc::Receiver<([u8; 16], AdminAudit)>,
    complete: mpsc::Sender<([u8; 16], std::io::Result<()>)>,
) {
    spawn_at(None, requests, complete);
}

fn spawn_at(
    target: Option<PathBuf>,
    mut requests: mpsc::Receiver<([u8; 16], AdminAudit)>,
    complete: mpsc::Sender<([u8; 16], std::io::Result<()>)>,
) {
    tokio::task::spawn_blocking(move || {
        while let Some((id, audit)) = requests.blocking_recv() {
            let result = if let Some(path) = target.as_ref() {
                append_to(path, id, &audit)
            } else {
                append(id, &audit)
            };
            if complete.blocking_send((id, result)).is_err() {
                break;
            }
        }
    });
}

fn append(id: [u8; 16], audit: &AdminAudit) -> std::io::Result<()> {
    append_to(&path()?, id, audit)
}

fn append_to(path: &std::path::Path, id: [u8; 16], audit: &AdminAudit) -> std::io::Result<()> {
    let created = !path.exists();
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&path)?;
    let offset = file.seek(SeekFrom::End(0))?;
    let bytes = encode_control_message(&AdminControlMessage::SequencedMutation {
        id,
        audit: audit.clone(),
    })
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if bytes.is_empty() || bytes.len() > MAX_RECORD {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "admin journal record too large",
        ));
    }
    let result = file
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .and_then(|()| file.write_all(&bytes))
        .and_then(|()| file.sync_all());
    if result.is_err() {
        file.set_len(offset)?;
        file.sync_all()?;
    } else if created {
        File::open(path.parent().unwrap())?.sync_all()?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::admin_sync::{AdminMutation, OpGrant};

    fn audit() -> AdminAudit {
        AdminAudit::new(
            String::from("Operator"),
            AdminMutation::GrantOp(OpGrant::new([2; 16], String::from("Target"), 4, false)),
        )
    }

    #[tokio::test]
    async fn synced_record_replays_after_restart_and_duplicate_id_is_known() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("admin.journal");
        let (request_tx, request_rx) = mpsc::channel(1);
        let (complete_tx, mut complete_rx) = mpsc::channel(1);
        spawn_at(Some(target.clone()), request_rx, complete_tx);
        request_tx.send(([7; 16], audit())).await.unwrap();
        let (id, result) = complete_rx.recv().await.unwrap();
        assert_eq!(id, [7; 16]);
        result.unwrap();
        drop(request_tx);

        let records = read_records(&target).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, audit());
        let mut restarted =
            AcceptedMutationIds::from_replay(records.into_iter().map(|(id, _)| id).collect());
        assert!(restarted.contains(&[7; 16]));
        restarted.insert([7; 16]);
        assert_eq!(restarted.order.len(), 1);
    }

    #[tokio::test]
    async fn failed_append_reports_error_without_durable_completion() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("missing").join("admin.journal");
        let (request_tx, request_rx) = mpsc::channel(1);
        let (complete_tx, mut complete_rx) = mpsc::channel(1);
        spawn_at(Some(target.clone()), request_rx, complete_tx);
        request_tx.send(([8; 16], audit())).await.unwrap();
        let (id, result) = complete_rx.recv().await.unwrap();
        assert_eq!(id, [8; 16]);
        assert!(result.is_err());
        assert!(!target.exists());
    }

    #[test]
    fn truncated_tail_is_removed_before_later_appends() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("admin.journal");
        append_to(&target, [1; 16], &audit()).unwrap();
        let original = std::fs::metadata(&target).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&target)
            .unwrap()
            .write_all(&[9, 9])
            .unwrap();
        assert_eq!(read_records(&target).unwrap().len(), 1);
        assert_eq!(std::fs::metadata(&target).unwrap().len(), original);
        append_to(&target, [2; 16], &audit()).unwrap();
        assert_eq!(read_records(&target).unwrap().len(), 2);
    }
}
