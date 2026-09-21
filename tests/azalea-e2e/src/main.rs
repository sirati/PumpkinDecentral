use std::{
    collections::HashSet,
    env, fs,
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use azalea::account::Account;
use azalea::{Client, Event, Vec3, WalkDirection, ecs::query::With, entity::metadata::Zombie};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, UdpSocket},
    process::{Child, Command},
    task::{JoinHandle, LocalSet},
    time,
};
use uuid::Uuid;

const BOT_A: &str = "PumpkinE2eA";
const BOT_B: &str = "PumpkinE2eB";
const E2E_DEADLINE: Duration = Duration::from_secs(90);
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);
const SNTP_EXCHANGE_TIMEOUT: Duration = Duration::from_millis(800);

struct Deadline {
    end: time::Instant,
}

impl Deadline {
    fn start() -> Self {
        Self {
            end: time::Instant::now() + E2E_DEADLINE,
        }
    }

    fn remaining(&self, stage: &str) -> Result<Duration> {
        self.end
            .checked_duration_since(time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .with_context(|| format!("Azalea E2E deadline expired during {stage}"))
    }
}

async fn within<T, F>(deadline: &Deadline, stage: &str, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let remaining = deadline.remaining(stage)?;
    time::timeout(remaining, future)
        .await
        .with_context(|| format!("Azalea E2E deadline expired during {stage}"))?
}

#[derive(Clone)]
struct Node {
    id: u16,
    role: &'static str,
    quic: String,
    java: Option<String>,
    root: PathBuf,
    pin: String,
}

struct Fixture {
    _root: TempDir,
    nodes: Vec<Node>,
    sntp: JoinHandle<()>,
}

struct Supervisor {
    children: Vec<Child>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let deadline = Deadline::start();
    let fixture = within(&deadline, "fixture creation", Fixture::create()).await?;
    let executable = pumpkin_executable()?;
    let mut supervisor = within(
        &deadline,
        "three-node process startup",
        Supervisor::start(&executable, &fixture.nodes),
    )
    .await?;
    let result = exercise(&fixture.nodes, &deadline).await;
    supervisor.shutdown().await;
    fixture.sntp.abort();
    result
}

impl Fixture {
    async fn create() -> Result<Self> {
        let root = tempfile::tempdir().context("creating E2E fixture directory")?;
        let (sntp_addr, sntp) = start_sntp().await?;
        let primary_quic = unused_udp_address().await?;
        let secondary_one_quic = unused_udp_address().await?;
        let secondary_two_quic = unused_udp_address().await?;
        let secondary_one_java = unused_tcp_address().await?;
        let secondary_two_java = unused_tcp_address().await?;
        let mut nodes = Vec::new();
        for (id, role, quic, java, directory) in [
            (0, "primary", primary_quic, None, "primary"),
            (
                1,
                "secondary",
                secondary_one_quic,
                Some(secondary_one_java),
                "secondary-1",
            ),
            (
                2,
                "secondary",
                secondary_two_quic,
                Some(secondary_two_java),
                "secondary-2",
            ),
        ] {
            let node_root = root.path().join(directory);
            fs::create_dir_all(&node_root)
                .with_context(|| format!("creating fixture node {}", node_root.display()))?;
            nodes.push(Node {
                id,
                role,
                quic,
                java,
                root: node_root,
                pin: write_keypair(&root.path().join(directory))?,
            });
        }
        for node in &nodes {
            write_config(node, &nodes, &sntp_addr)?;
        }
        write_ops(&nodes[0].root)?;
        Ok(Self {
            _root: root,
            nodes,
            sntp,
        })
    }
}

impl Supervisor {
    async fn start(executable: &Path, nodes: &[Node]) -> Result<Self> {
        let mut children = Vec::with_capacity(nodes.len());
        for node in nodes {
            let child = Command::new(executable)
                .current_dir(&node.root)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .with_context(|| format!("starting {} node", node.role));
            match child {
                Ok(child) => children.push(child),
                Err(error) => {
                    let mut supervisor = Self { children };
                    supervisor.shutdown().await;
                    return Err(error);
                }
            }
        }
        Ok(Self { children })
    }

    async fn shutdown(&mut self) {
        for child in &mut self.children {
            let _ = child.start_kill();
        }
        let _ = time::timeout(CLEANUP_TIMEOUT, async {
            for child in &mut self.children {
                let _ = child.wait().await;
            }
        })
        .await;
    }
}

async fn exercise(nodes: &[Node], deadline: &Deadline) -> Result<()> {
    let one = nodes
        .iter()
        .find(|node| node.id == 1)
        .context("missing secondary one")?;
    let two = nodes
        .iter()
        .find(|node| node.id == 2)
        .context("missing secondary two")?;
    let ((bot_a, mut a_events), (bot_b, mut b_events)) = tokio::try_join!(
        connect(
            BOT_A,
            one.java
                .as_deref()
                .context("secondary one Java listener missing")?,
            deadline,
        ),
        connect(
            BOT_B,
            two.java
                .as_deref()
                .context("secondary two Java listener missing")?,
            deadline,
        ),
    )?;
    wait_login(&mut a_events, deadline, "secondary one login").await?;
    wait_login(&mut b_events, deadline, "secondary two login").await?;
    tokio::try_join!(
        wait_virtual_lobby_spawn(&mut a_events, deadline, "secondary one virtual lobby"),
        wait_virtual_lobby_spawn(&mut b_events, deadline, "secondary two virtual lobby"),
    )?;
    let bot_b_uuid = wait_for_player(&bot_a, BOT_B, deadline, "remote player B handoff")
        .await
        .map_err(|error| azalea_disconnect_context(error, &mut a_events))?;
    let bot_a_uuid = wait_for_player(&bot_b, BOT_A, deadline, "remote player A handoff")
        .await
        .map_err(|error| azalea_disconnect_context(error, &mut b_events))?;
    let before_move = player_position(&bot_b, bot_a_uuid)?;
    bot_a.walk(WalkDirection::Forward);
    wait_ticks(&bot_a, 5, deadline, "local movement").await?;
    bot_a.walk(WalkDirection::None);
    wait_for_position_change(
        &bot_b,
        bot_a_uuid,
        before_move,
        deadline,
        "remote movement replication",
    )
    .await?;
    wait_for_player(&bot_a, BOT_B, deadline, "remote player B after movement").await?;
    tokio::try_join!(
        async {
            fight_until_death_message(&bot_a, bot_b_uuid, &mut a_events, deadline).await?;
            summon_secondary_owned_zombie(&bot_a, &bot_b, deadline).await
        },
        observe_primary_natural_spawn_boundary(&bot_a, deadline),
    )?;
    bot_a.disconnect();
    bot_b.disconnect();
    Ok(())
}

fn azalea_disconnect_context(
    error: anyhow::Error,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
) -> anyhow::Error {
    let mut disconnects = Vec::new();
    let mut last_packet = None;
    while let Ok(event) = events.try_recv() {
        match event {
            Event::Disconnect(reason) => {
                disconnects.push(reason.map_or_else(
                    || String::from("connection closed"),
                    |text| text.to_string(),
                ));
            }
            Event::Packet(packet) => {
                last_packet = Some(format!("{:?}", std::mem::discriminant(packet.as_ref())));
            }
            _ => {}
        }
    }
    if disconnects.is_empty() && last_packet.is_none() {
        error
    } else {
        let mut detail = disconnects.join("; ");
        if let Some(packet) = last_packet {
            if !detail.is_empty() {
                detail.push_str("; ");
            }
            detail.push_str(&format!("last clientbound packet {packet}"));
        }
        error.context(format!("Azalea connection state: {detail}"))
    }
}

async fn connect(
    name: &str,
    address: &str,
    deadline: &Deadline,
) -> Result<(Client, tokio::sync::mpsc::UnboundedReceiver<Event>)> {
    let last_error = loop {
        let remaining = deadline.remaining(&format!("connecting {name}"))?;
        let error = match time::timeout(
            remaining.min(CONNECT_ATTEMPT_TIMEOUT),
            Client::join(Account::offline(name), address),
        )
        .await
        {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(error)) => error.to_string(),
            Err(_) => String::from("connection attempt timed out"),
        };
        let remaining = deadline.remaining(&format!("connecting {name}"))?;
        if remaining <= CONNECT_RETRY_DELAY {
            break error;
        }
        time::sleep(CONNECT_RETRY_DELAY).await;
    };
    bail!("Azalea could not connect {name}: {last_error}")
}

async fn wait_login(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            if matches!(event, Event::Login) {
                return Ok(());
            }
        }
        bail!("Azalea event stream closed before login")
    })
    .await
    .with_context(|| format!("Azalea login did not complete on {stage}"))
}

async fn wait_virtual_lobby_spawn(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Spawn => return Ok(()),
                Event::Disconnect(reason) => {
                    bail!(
                        "Azalea disconnected in the virtual lobby: {}",
                        reason.map_or_else(
                            || String::from("connection closed"),
                            |text| text.to_string()
                        )
                    );
                }
                _ => {}
            }
        }
        bail!("Azalea event stream closed before virtual lobby spawn")
    })
    .await
    .with_context(|| format!("Azalea virtual lobby did not establish a client world on {stage}"))
}

async fn wait_for_player(
    client: &Client,
    name: &str,
    deadline: &Deadline,
    stage: &str,
) -> Result<Uuid> {
    let name = String::from(name);
    wait_until(client, deadline, stage, move || {
        let uuid = client
            .player_uuid_by_username(&name)
            .context("reading Azalea tab list")?;
        Ok(uuid.filter(|uuid| client.entity_id_by_uuid(*uuid).is_some()))
    })
    .await
}

async fn wait_for_position_change(
    client: &Client,
    uuid: Uuid,
    before: Vec3,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    wait_until(client, deadline, stage, || {
        let Some(entity) = client.entity_by_uuid(uuid) else {
            return Ok(None);
        };
        let position = entity
            .position()
            .context("reading remote player position")?;
        Ok(position_changed(position, before).then_some(()))
    })
    .await
}

fn position_changed(left: Vec3, right: Vec3) -> bool {
    (left.x - right.x).abs() > 0.01
        || (left.y - right.y).abs() > 0.01
        || (left.z - right.z).abs() > 0.01
}

async fn wait_until<T, F>(
    client: &Client,
    deadline: &Deadline,
    stage: &str,
    mut condition: F,
) -> Result<T>
where
    F: FnMut() -> Result<Option<T>>,
{
    within(deadline, stage, async {
        let mut ticks = client.get_tick_broadcaster();
        while ticks.recv().await.is_ok() {
            if let Some(value) = condition()? {
                return Ok(value);
            }
        }
        bail!("Azalea tick stream closed")
    })
    .await
    .with_context(|| format!("Azalea state condition did not complete during {stage}"))
}

async fn wait_ticks(client: &Client, count: usize, deadline: &Deadline, stage: &str) -> Result<()> {
    within(deadline, stage, async {
        let mut ticks = client.get_tick_broadcaster();
        for _ in 0..count {
            ticks.recv().await.context("Azalea tick stream closed")?;
        }
        Ok(())
    })
    .await
}

fn player_position(client: &Client, uuid: Uuid) -> Result<Vec3> {
    client
        .entity_by_uuid(uuid)
        .context("remote player is absent from the client world")?
        .position()
        .context("reading remote player position")
}

async fn fight_until_death_message(
    attacker: &Client,
    target_uuid: Uuid,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    within(deadline, "cross-node combat and death", async {
        for _ in 0..8 {
            let target = attacker
                .entity_by_uuid(target_uuid)
                .context("combat target left the attacking client world")?;
            attacker.look_at(
                target
                    .eye_position()
                    .context("reading combat target eye position")?,
            );
            let entity = attacker
                .entity_id_by_uuid(target_uuid)
                .context("combat target entity id is absent")?;
            if !attacker.has_attack_cooldown() {
                attacker.attack(entity);
            }
            wait_ticks(attacker, 2, deadline, "cross-node combat and death").await?;
            while let Ok(event) = events.try_recv() {
                if let Event::Chat(chat) = event {
                    let message = chat.message().to_string();
                    if message.contains(BOT_A) && message.contains(BOT_B) {
                        return Ok(());
                    }
                }
            }
        }
        bail!("direct Azalea creative attacks did not produce a cross-client death message")
    })
    .await
}

async fn summon_secondary_owned_zombie(
    summoner: &Client,
    observer: &Client,
    deadline: &Deadline,
) -> Result<()> {
    within(
        deadline,
        "secondary-owned summoned entity replication",
        async {
            let existing = zombie_uuids(observer)?;
            summoner.chat("/summon minecraft:zombie ~ ~ ~");
            let zombie_uuid = wait_until(observer, deadline, "summoned entity replication", || {
                let zombies = observer
                    .nearest_entities::<With<Zombie>>()
                    .context("searching observer world for summoned zombie")?;
                for zombie in zombies {
                    let uuid = zombie.uuid().context("reading summoned zombie UUID")?;
                    if !existing.contains(&uuid) {
                        return Ok(Some(uuid));
                    }
                }
                Ok(None)
            })
            .await?;
            let before = observer
                .entity_by_uuid(zombie_uuid)
                .context("summoned zombie left observer world")?
                .position()
                .context("reading summoned zombie position")?;
            wait_for_position_change(
                observer,
                zombie_uuid,
                before,
                deadline,
                "summoned entity replication movement",
            )
            .await?;
            eprintln!("secondary-owned deterministic mob movement observed through Azalea");
            Ok(())
        },
    )
    .await
}

fn zombie_uuids(client: &Client) -> Result<HashSet<Uuid>> {
    client
        .nearest_entities::<With<Zombie>>()
        .context("reading observer zombie UUIDs")?
        .into_iter()
        .map(|entity| entity.uuid().context("reading observer zombie UUID"))
        .collect()
}

async fn observe_primary_natural_spawn_boundary(
    client: &Client,
    deadline: &Deadline,
) -> Result<()> {
    client.chat("/time set midnight");
    wait_ticks(
        client,
        200,
        deadline,
        "primary natural-spawn observation boundary",
    )
    .await?;
    eprintln!(
        "primary-natural-spawn observation remains unproven: Java client entity packets do not encode cluster owner"
    );
    Ok(())
}

fn pumpkin_executable() -> Result<PathBuf> {
    if let Some(path) = env::var_os("PUMPKIN_E2E_PUMPKIN_BIN") {
        return Ok(PathBuf::from(path));
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .context("locating repository root")?;
    Ok(root.join("result-dev/bin/pumpkin"))
}

async fn unused_tcp_address() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserving an E2E Java listener address")?;
    Ok(listener
        .local_addr()
        .context("reading reserved E2E Java listener address")?
        .to_string())
}

async fn unused_udp_address() -> Result<String> {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .context("reserving an E2E QUIC address")?;
    Ok(socket
        .local_addr()
        .context("reading reserved E2E QUIC address")?
        .to_string())
}

async fn start_sntp() -> Result<(String, JoinHandle<()>)> {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .context("binding local SNTP responder")?;
    let address = socket
        .local_addr()
        .context("reading local SNTP responder address")?;
    let task = tokio::spawn(async move {
        let mut request = [0_u8; 48];
        loop {
            let Ok((length, peer)) = socket.recv_from(&mut request).await else {
                return;
            };
            if length != request.len() {
                continue;
            }
            let Some(received) = sntp_timestamp() else {
                continue;
            };
            let Some(transmitted) = sntp_timestamp() else {
                continue;
            };
            let mut reply = [0_u8; 48];
            reply[0] = 0x24;
            reply[1] = 1;
            reply[2] = 6;
            reply[3] = (-20_i8) as u8;
            reply[12..16].copy_from_slice(b"LOCL");
            reply[16..24].copy_from_slice(&received);
            reply[24..32].copy_from_slice(&request[40..48]);
            reply[32..40].copy_from_slice(&received);
            reply[40..48].copy_from_slice(&transmitted);
            let _ = socket.send_to(&reply, peer).await;
        }
    });
    if let Err(error) = prove_sntp_exchange(address).await {
        task.abort();
        return Err(error);
    }
    Ok((address.to_string(), task))
}

async fn prove_sntp_exchange(server: SocketAddr) -> Result<()> {
    let bind = if server.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind)
        .await
        .context("binding production-shaped SNTP fixture client")?;
    socket
        .connect(server)
        .await
        .context("connecting production-shaped SNTP fixture client")?;
    let (request, transmitted) =
        sntp_request().context("creating production-shaped SNTP fixture request")?;
    socket
        .send(&request)
        .await
        .context("sending production-shaped SNTP fixture request")?;
    let mut reply = [0_u8; 512];
    let length = time::timeout(SNTP_EXCHANGE_TIMEOUT, socket.recv(&mut reply))
        .await
        .context("local SNTP responder did not answer a production-shaped exchange")?
        .context("receiving local SNTP responder reply")?;
    if length < 48 {
        bail!("local SNTP responder returned a short {length}-byte reply");
    }
    if reply[0] & 0x07 != 4 {
        bail!(
            "local SNTP responder returned non-server mode {}",
            reply[0] & 0x07
        );
    }
    if reply[1] == 0 {
        bail!("local SNTP responder returned a kiss-of-death reply");
    }
    if reply[24..32] != transmitted {
        bail!("local SNTP responder did not preserve the request origin timestamp");
    }
    if reply[32..40].iter().all(|byte| *byte == 0) || reply[40..48].iter().all(|byte| *byte == 0) {
        bail!("local SNTP responder returned an incomplete server timestamp pair");
    }
    Ok(())
}

fn sntp_timestamp() -> Option<[u8; 8]> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let seconds = u32::try_from(elapsed.as_secs().checked_add(2_208_988_800)?).ok()?;
    let fraction = u64::from(elapsed.subsec_nanos()).checked_shl(32)? / 1_000_000_000;
    Some(((u64::from(seconds) << 32) | fraction).to_be_bytes())
}

fn sntp_request() -> Option<([u8; 48], [u8; 8])> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let millis = i64::try_from(elapsed.as_millis()).ok()?;
    let seconds = millis.div_euclid(1_000).checked_add(2_208_988_800)?;
    let fraction = u64::try_from(millis.rem_euclid(1_000))
        .ok()?
        .checked_mul(4_294_967_296)?
        .checked_add(500)?
        / 1_000;
    let seconds = u32::try_from(seconds).ok()?;
    let fraction = u32::try_from(fraction).ok()?;
    let transmitted = ((u64::from(seconds) << 32) | u64::from(fraction)).to_be_bytes();
    let mut request = [0_u8; 48];
    request[0] = 0x1B;
    request[40..48].copy_from_slice(&transmitted);
    Some((request, transmitted))
}

fn write_keypair(node_root: &Path) -> Result<String> {
    let keypair = rcgen::generate_simple_self_signed(vec![String::from("pumpkin-mesh")])
        .context("generating mesh certificate")?;
    let certificate = keypair.cert.der().to_vec();
    let key = keypair.signing_key.serialize_der();
    fs::write(node_root.join("cluster-cert.der"), &certificate)
        .with_context(|| format!("writing certificate in {}", node_root.display()))?;
    fs::write(node_root.join("cluster-key.der"), key)
        .with_context(|| format!("writing key in {}", node_root.display()))?;
    Ok(hex::encode(Sha256::digest(certificate)))
}

fn write_config(node: &Node, nodes: &[Node], sntp: &str) -> Result<()> {
    let peers = nodes
        .iter()
        .filter(|peer| peer.id != node.id)
        .map(|peer| {
            format!(
                "{{ server_id = {}, addr = \"{}\", pubkey_sha256_hex = \"{}\" }}",
                peer.id, peer.quic, peer.pin
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let java_enabled = node.java.is_some();
    let java_address = node.java.as_deref().unwrap_or("127.0.0.1:0");
    let save_player_data = node.role == "primary";
    let config = format!(
        "seed = \"1787949120387005756\"\ndefault_level_name = \"world\"\ndefault_gamemode = \"Creative\"\nforce_gamemode = true\nspawn_protection = 0\n\n[world]\nautosave_ticks = 6000\n\n[world.chunk]\ntype = \"anvil\"\nwrite_in_place = false\n\n[networking.java]\nenabled = {java_enabled}\naddress = \"{java_address}\"\nonline_mode = false\nencryption = false\nview_distance = 2\nsimulation_distance = 2\n\n[networking.bedrock]\nenabled = false\n\n[networking.bedrock.nethernet]\nenabled = false\naddress = \"127.0.0.1:19132\"\n\n[networking.query]\nenabled = false\naddress = \"127.0.0.1:25565\"\n\n[networking.rcon]\nenabled = false\naddress = \"127.0.0.1:25575\"\n\n[networking.lan_broadcast]\nenabled = false\n\n[player_data]\nsave_player_data = {save_player_data}\n\n[cluster]\nenabled = true\nrole = \"{}\"\nserver_id = {}\nprimary_server_id = 0\nbind_addr = \"{}\"\ncert_path = \"cluster-cert.der\"\nkey_path = \"cluster-key.der\"\npeers = [{peers}]\nntp_servers = [\"{sntp}\"]\nmax_precision_millis = 25\n\n[telemetry]\nenabled = false\n",
        node.role, node.id, node.quic
    );
    fs::write(node.root.join("pumpkin.toml"), config)
        .with_context(|| format!("writing {} configuration", node.role))
}

fn write_ops(primary_root: &Path) -> Result<()> {
    let data = primary_root.join("data");
    fs::create_dir_all(&data).with_context(|| format!("creating {}", data.display()))?;
    let ops = [BOT_A, BOT_B]
        .into_iter()
        .map(|name| {
            serde_json::json!({
                "uuid": offline_uuid(name),
                "name": name,
                "level": 4,
                "bypasses_player_limit": true,
            })
        })
        .collect::<Vec<_>>();
    fs::write(
        data.join("ops.json"),
        serde_json::to_vec_pretty(&serde_json::json!({ "ops": ops }))
            .context("serializing deterministic offline operators")?,
    )
    .with_context(|| format!("writing {}", data.join("ops.json").display()))
}

fn offline_uuid(name: &str) -> Uuid {
    let digest = Sha256::digest(name.as_bytes());
    Uuid::from_slice(&digest[..16]).expect("SHA-256 prefix has UUID length")
}
