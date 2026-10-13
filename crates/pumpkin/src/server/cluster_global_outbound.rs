use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use pumpkin_cluster::admin_sync::{AdminControlMessage, decode_control_message};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use tokio::sync::mpsc;

use super::Server;

const PENDING_LIMIT: usize = 4096;

pub(crate) struct GlobalOutboundRuntime {
    tx: mpsc::Sender<Command>,
    ack_tx: mpsc::Sender<[u8; 16]>,
}

pub(crate) struct EventReservation {
    permit: mpsc::OwnedPermit<Command>,
    route: EventRoute,
}

pub(crate) struct SnapshotReservation {
    permit: mpsc::OwnedPermit<Command>,
    target: (ServerId, u64),
}

impl SnapshotReservation {
    pub(crate) fn commit(self, frames: VecDeque<Vec<u8>>) {
        self.permit.send(Command::Snapshot {
            target: self.target,
            frames,
        });
    }
}

pub(crate) enum EventRoute {
    Fixed(Vec<(ServerId, u64)>),
    PrimaryReliable([u8; 16]),
}

impl EventReservation {
    pub(crate) fn commit(self, bytes: Vec<u8>) {
        self.permit.send(Command::Event {
            route: self.route,
            bytes,
        });
    }
}

impl GlobalOutboundRuntime {
    #[must_use]
    pub(crate) fn new() -> (Self, mpsc::Receiver<Command>, mpsc::Receiver<[u8; 16]>) {
        let (tx, rx) = mpsc::channel(PENDING_LIMIT);
        let (ack_tx, ack_rx) = mpsc::channel(PENDING_LIMIT);
        (Self { tx, ack_tx }, rx, ack_rx)
    }

    pub(crate) fn configure(&self, outbound: mpsc::Sender<OutboundParcel>) -> bool {
        self.tx.try_send(Command::Configure(outbound)).is_ok()
    }

    pub(crate) fn try_event(&self, server: &Server, peers: Vec<ServerId>, bytes: Vec<u8>) -> bool {
        let Some(targets) = Self::targets(server, peers) else {
            return false;
        };
        self.tx
            .try_send(Command::Event {
                route: EventRoute::Fixed(targets),
                bytes,
            })
            .is_ok()
    }

    pub(crate) fn try_reserve_primary_event(&self, id: [u8; 16]) -> Option<EventReservation> {
        let permit = self.tx.clone().try_reserve_owned().ok()?;
        Some(EventReservation {
            permit,
            route: EventRoute::PrimaryReliable(id),
        })
    }

    pub(crate) fn try_reserve_event(
        &self,
        server: &Server,
        peers: Vec<ServerId>,
    ) -> Option<EventReservation> {
        let targets = Self::targets(server, peers)?;
        Some(EventReservation {
            permit: self.tx.clone().try_reserve_owned().ok()?,
            route: EventRoute::Fixed(targets),
        })
    }

    pub(crate) fn acknowledge_primary_event(&self, id: [u8; 16]) -> bool {
        self.ack_tx.try_send(id).is_ok()
    }

    pub(crate) fn try_reserve_snapshot(
        &self,
        server: &Server,
        peer: ServerId,
    ) -> Option<SnapshotReservation> {
        let Some(mut targets) = Self::targets(server, vec![peer]) else {
            return None;
        };
        Some(SnapshotReservation {
            permit: self.tx.clone().try_reserve_owned().ok()?,
            target: targets.remove(0),
        })
    }

    fn targets(server: &Server, peers: Vec<ServerId>) -> Option<Vec<(ServerId, u64)>> {
        let generations = server.cluster_peer_generations.load();
        let connected = server.cluster_connected_peers.load();
        peers
            .into_iter()
            .map(|peer| {
                connected
                    .contains(&peer)
                    .then_some((peer, generations.get(&peer).copied().unwrap_or(0)))
            })
            .collect()
    }
}

pub(crate) enum Command {
    Configure(mpsc::Sender<OutboundParcel>),
    Event {
        route: EventRoute,
        bytes: Vec<u8>,
    },
    Snapshot {
        target: (ServerId, u64),
        frames: VecDeque<Vec<u8>>,
    },
}

struct ReliablePrimaryEvent {
    id: [u8; 16],
    bytes: Vec<u8>,
    sent_to: Option<(ServerId, u64)>,
    issuer: Option<String>,
}

struct Owner {
    outbound: Option<mpsc::Sender<OutboundParcel>>,
    events: VecDeque<(u64, OutboundParcel)>,
    primary_events: VecDeque<ReliablePrimaryEvent>,
    snapshots: VecDeque<((ServerId, u64), VecDeque<Vec<u8>>)>,
}

impl Owner {
    fn new() -> Self {
        Self {
            outbound: None,
            events: VecDeque::new(),
            primary_events: VecDeque::new(),
            snapshots: VecDeque::new(),
        }
    }

    fn pending(&self) -> usize {
        self.events
            .len()
            .saturating_add(self.primary_events.len())
            .saturating_add(self.snapshots.len())
    }

    fn acknowledge(&mut self, id: [u8; 16]) -> Option<String> {
        let position = self
            .primary_events
            .iter()
            .position(|event| event.id == id)?;
        self.primary_events.remove(position)?.issuer
    }

    fn insert_event(&mut self, peer: ServerId, generation: u64, bytes: Vec<u8>) {
        self.events.push_back((
            generation,
            OutboundParcel {
                peer,
                header: StreamHeader::new(StreamKind::Control, None),
                bytes,
            },
        ));
    }

    fn accept(&mut self, command: Command) {
        match command {
            Command::Configure(outbound) => self.outbound = Some(outbound),
            Command::Snapshot { target, frames } => {
                if let Some(existing) = self.snapshots.iter_mut().find(|(old, _)| old.0 == target.0)
                {
                    *existing = (target, frames);
                } else {
                    self.snapshots.push_back((target, frames));
                }
            }
            Command::Event { route, bytes } => match route {
                EventRoute::Fixed(targets) => {
                    for (peer, generation) in targets {
                        self.insert_event(peer, generation, bytes.clone());
                    }
                }
                EventRoute::PrimaryReliable(id) => {
                    let issuer = match decode_control_message(&bytes) {
                        Ok(AdminControlMessage::SequencedMutation { audit, .. }) => {
                            Some(audit.issuer)
                        }
                        _ => None,
                    };
                    self.primary_events.push_back(ReliablePrimaryEvent {
                        id,
                        bytes,
                        sent_to: None,
                        issuer,
                    })
                }
            },
        }
    }

    fn flush(
        &mut self,
        generations: &std::collections::HashMap<ServerId, u64>,
        connected: &std::collections::HashSet<ServerId>,
        primaries: &std::collections::HashSet<ServerId>,
    ) {
        let Some(outbound) = self.outbound.as_ref() else {
            return;
        };
        while let Some(((peer, generation), mut frames)) = self.snapshots.pop_front() {
            if !connected.contains(&peer)
                || generations.get(&peer).copied().unwrap_or(0) != generation
            {
                continue;
            }
            while let Some(bytes) = frames.pop_front() {
                match outbound.try_send(OutboundParcel {
                    peer,
                    header: StreamHeader::new(StreamKind::Control, None),
                    bytes,
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(parcel))
                    | Err(mpsc::error::TrySendError::Closed(parcel)) => {
                        frames.push_front(parcel.bytes);
                        self.snapshots.push_front(((peer, generation), frames));
                        return;
                    }
                }
            }
        }
        if let Some(primary) = primaries
            .iter()
            .copied()
            .filter(|peer| connected.contains(peer))
            .min_by_key(|peer| peer.0)
        {
            let generation = generations.get(&primary).copied().unwrap_or(0);
            for event in &mut self.primary_events {
                if event.sent_to == Some((primary, generation)) {
                    continue;
                }
                match outbound.try_send(OutboundParcel {
                    peer: primary,
                    header: StreamHeader::new(StreamKind::Control, None),
                    bytes: event.bytes.clone(),
                }) {
                    Ok(()) => event.sent_to = Some((primary, generation)),
                    Err(mpsc::error::TrySendError::Full(_))
                    | Err(mpsc::error::TrySendError::Closed(_)) => return,
                }
            }
        }
        while let Some((generation, parcel)) = self.events.pop_front() {
            if !connected.contains(&parcel.peer)
                || generations.get(&parcel.peer).copied().unwrap_or(0) != generation
            {
                continue;
            }
            match outbound.try_send(parcel) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(parcel)) => {
                    self.events.push_front((generation, parcel));
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(parcel)) => {
                    self.events.push_front((generation, parcel));
                    return;
                }
            }
        }
    }
}

pub(crate) fn spawn(
    server: &Arc<Server>,
    mut rx: mpsc::Receiver<Command>,
    mut ack_rx: mpsc::Receiver<[u8; 16]>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut owner = Owner::new();
        let mut flush = tokio::time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                Some(id) = ack_rx.recv() => {
                    if let Some(issuer) = owner.acknowledge(id) {
                        super::cluster_admin_apply::notify_durable_admin_ack(&task_server, &issuer);
                    }
                },
                command = rx.recv(), if owner.pending() < PENDING_LIMIT => {
                    let Some(command) = command else {
                        break;
                    };
                    owner.accept(command);
                }
                _ = flush.tick() => owner.flush(
                    &task_server.cluster_peer_generations.load(),
                    &task_server.cluster_connected_peers.load(),
                    &task_server.cluster_primary_peers.load(),
                ),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn distinct_events_survive_transport_backpressure_in_order() {
        let (transport, mut transport_rx) = mpsc::channel(1);
        transport
            .try_send(OutboundParcel {
                peer: ServerId(9),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![0],
            })
            .unwrap();
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Event {
            route: EventRoute::Fixed(vec![(ServerId(2), 1)]),
            bytes: vec![1],
        });
        owner.accept(Command::Event {
            route: EventRoute::Fixed(vec![(ServerId(3), 1)]),
            bytes: vec![3],
        });
        let generations = std::collections::HashMap::from([(ServerId(2), 1), (ServerId(3), 1)]);
        let connected = std::collections::HashSet::from([ServerId(2), ServerId(3)]);
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(owner.pending(), 2);
        transport_rx.recv().await.unwrap();
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![1]);
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![3]);
        assert_eq!(owner.pending(), 0);
    }

    #[tokio::test]
    async fn queued_control_for_old_connection_is_not_sent_to_replacement() {
        let (transport, mut transport_rx) = mpsc::channel(1);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Event {
            route: EventRoute::Fixed(vec![(ServerId(2), 1)]),
            bytes: vec![7],
        });
        owner.flush(
            &std::collections::HashMap::from([(ServerId(2), 2)]),
            &std::collections::HashSet::from([ServerId(2)]),
            &std::collections::HashSet::new(),
        );
        assert!(transport_rx.try_recv().is_err());
        assert_eq!(owner.pending(), 0);
    }

    #[tokio::test]
    async fn first_connection_without_drop_epoch_receives_control() {
        let (transport, mut transport_rx) = mpsc::channel(1);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Event {
            route: EventRoute::Fixed(vec![(ServerId(2), 0)]),
            bytes: vec![8],
        });
        owner.flush(
            &std::collections::HashMap::new(),
            &std::collections::HashSet::from([ServerId(2)]),
            &std::collections::HashSet::new(),
        );
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![8]);
    }

    #[tokio::test]
    async fn closed_transport_keeps_event_until_reconfigured() {
        let (closed_transport, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let (replacement, mut replacement_rx) = mpsc::channel(1);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(closed_transport));
        owner.accept(Command::Event {
            route: EventRoute::Fixed(vec![(ServerId(2), 0)]),
            bytes: vec![9],
        });
        let connected = std::collections::HashSet::from([ServerId(2)]);
        owner.flush(
            &std::collections::HashMap::new(),
            &connected,
            &std::collections::HashSet::new(),
        );
        owner.accept(Command::Configure(replacement));
        owner.flush(
            &std::collections::HashMap::new(),
            &connected,
            &std::collections::HashSet::new(),
        );
        assert_eq!(replacement_rx.recv().await.unwrap().bytes, vec![9]);
    }

    #[tokio::test]
    async fn admin_event_waits_for_primary_without_blocking_other_events() {
        let (transport, mut transport_rx) = mpsc::channel(2);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Event {
            route: EventRoute::PrimaryReliable([1; 16]),
            bytes: vec![10],
        });
        owner.accept(Command::Event {
            route: EventRoute::Fixed(vec![(ServerId(3), 0)]),
            bytes: vec![11],
        });
        let connected = std::collections::HashSet::from([ServerId(3)]);
        owner.flush(
            &std::collections::HashMap::new(),
            &connected,
            &std::collections::HashSet::new(),
        );
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![11]);
        assert_eq!(owner.pending(), 1);
        let connected = std::collections::HashSet::from([ServerId(2), ServerId(3)]);
        owner.flush(
            &std::collections::HashMap::new(),
            &connected,
            &std::collections::HashSet::from([ServerId(2)]),
        );
        let delivered = transport_rx.recv().await.unwrap();
        assert_eq!(delivered.peer, ServerId(2));
        assert_eq!(delivered.bytes, vec![10]);
        assert_eq!(owner.pending(), 1);
        let _ = owner.acknowledge([1; 16]);
        assert_eq!(owner.pending(), 0);
    }

    #[tokio::test]
    async fn unacknowledged_admin_mutation_resends_only_after_primary_replacement() {
        let (transport, mut transport_rx) = mpsc::channel(2);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Event {
            route: EventRoute::PrimaryReliable([9; 16]),
            bytes: vec![42],
        });
        let connected = std::collections::HashSet::from([ServerId(2)]);
        let primaries = connected.clone();
        owner.flush(
            &std::collections::HashMap::from([(ServerId(2), 1)]),
            &connected,
            &primaries,
        );
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![42]);
        owner.flush(
            &std::collections::HashMap::from([(ServerId(2), 1)]),
            &connected,
            &primaries,
        );
        assert!(transport_rx.try_recv().is_err());
        owner.flush(
            &std::collections::HashMap::from([(ServerId(2), 2)]),
            &connected,
            &primaries,
        );
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![42]);
        let _ = owner.acknowledge([9; 16]);
        owner.flush(
            &std::collections::HashMap::from([(ServerId(2), 3)]),
            &connected,
            &primaries,
        );
        assert!(transport_rx.try_recv().is_err());
    }

    #[test]
    fn acknowledged_admin_mutation_reports_issuer_once() {
        let bytes = pumpkin_cluster::admin_sync::encode_control_message(
            &AdminControlMessage::SequencedMutation {
                id: [5; 16],
                audit: pumpkin_cluster::admin_sync::AdminAudit::new(
                    String::from("Operator"),
                    pumpkin_cluster::admin_sync::AdminMutation::RevokeOp(
                        pumpkin_cluster::admin_sync::OpRevoke::new([2; 16], String::from("Target")),
                    ),
                ),
            },
        )
        .unwrap();
        let mut owner = Owner::new();
        owner.accept(Command::Event {
            route: EventRoute::PrimaryReliable([5; 16]),
            bytes,
        });
        assert_eq!(owner.acknowledge([5; 16]).as_deref(), Some("Operator"));
        assert!(owner.acknowledge([5; 16]).is_none());
    }

    #[tokio::test]
    async fn snapshot_frames_survive_backpressure_in_order() {
        let (transport, mut transport_rx) = mpsc::channel(1);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Snapshot {
            target: (ServerId(2), 4),
            frames: VecDeque::from([vec![1], vec![2], vec![3]]),
        });
        let generations = std::collections::HashMap::from([(ServerId(2), 4)]);
        let connected = std::collections::HashSet::from([ServerId(2)]);
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![1]);
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![2]);
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![3]);
        assert_eq!(owner.pending(), 0);
    }

    #[tokio::test]
    async fn later_snapshot_supersedes_unsent_state_for_same_peer() {
        let (transport, mut transport_rx) = mpsc::channel(2);
        let mut owner = Owner::new();
        owner.accept(Command::Configure(transport));
        owner.accept(Command::Snapshot {
            target: (ServerId(2), 4),
            frames: VecDeque::from([vec![1], vec![2]]),
        });
        owner.accept(Command::Snapshot {
            target: (ServerId(2), 5),
            frames: VecDeque::from([vec![3], vec![4]]),
        });
        let generations = std::collections::HashMap::from([(ServerId(2), 5)]);
        let connected = std::collections::HashSet::from([ServerId(2)]);
        owner.flush(&generations, &connected, &std::collections::HashSet::new());
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![3]);
        assert_eq!(transport_rx.recv().await.unwrap().bytes, vec![4]);
        assert_eq!(owner.pending(), 0);
    }
}
