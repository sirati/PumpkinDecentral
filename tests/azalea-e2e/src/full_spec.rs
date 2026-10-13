use std::{collections::BTreeSet, env, fs, path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde_json::{Value, json};
use tokio::{
    process::{Child, Command},
    time::timeout,
};

mod scenario_catalog;

struct Case {
    id: &'static str,
    scenario: Option<&'static str>,
    environment: Vec<(&'static str, String)>,
}

fn cases(recovered_limit: &str) -> Vec<Case> {
    let mut cases = vec![
        Case {
            id: "baseline",
            scenario: None,
            environment: vec![],
        },
        Case {
            id: "normal-feature-omission",
            scenario: None,
            environment: vec![],
        },
        Case {
            id: "full-radius-recovered-latency",
            scenario: None,
            environment: vec![
                ("PUMPKIN_E2E_FULL_RADIUS", "1".into()),
                ("PUMPKIN_E2E_ONE_WAY_DELAY_MS", "140".into()),
                (
                    "PUMPKIN_E2E_RECOVERED_HANDOFF_MAX_MS",
                    recovered_limit.into(),
                ),
            ],
        },
        Case {
            id: "early-chunk-streaming",
            scenario: None,
            environment: vec![("PUMPKIN_E2E_DELAY_CHUNK", "0,0,1,3000".into())],
        },
        Case {
            id: "large-admin-snapshot",
            scenario: None,
            environment: vec![("PUMPKIN_E2E_LARGE_ADMIN", "1".into())],
        },
    ];
    cases.extend(scenario_catalog::REQUIRED_SCENARIOS.iter().map(|id| Case {
        id,
        scenario: Some(id),
        environment: vec![],
    }));
    cases
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() == 1 && arguments[0] == "--list" {
        for case in cases("required") {
            let topology = if case.id == "normal-feature-omission" {
                "isolated normal-feature secondary"
            } else if case.id == "relay-drop-newest-ground-election" {
                "one primary, two player secondaries and two holder secondaries"
            } else if case.id == "multi-primary-no-coholder" {
                "two primaries and two secondaries"
            } else {
                "one primary and two secondaries"
            };
            println!(
                "mandatory {}: {topology}; fresh fixture invocation",
                case.id
            );
        }
        return Ok(());
    }
    if arguments.len() != 5 {
        bail!(
            "usage: pumpkin-cluster-full-spec <fixture-bin> <cluster-e2e-server-bin> <normal-server-bin> <recovered-handoff-max-ms> <new-report-directory>; --list enumerates mandatory cases without running servers"
        );
    }
    let fixture = executable(&arguments[0])?;
    let feature_server = executable(&arguments[1])?;
    let normal_server = executable(&arguments[2])?;
    if feature_server == normal_server {
        bail!("normal-feature omission requires a separately built normal server");
    }
    let recovered_limit = arguments[3]
        .to_str()
        .context("recovered latency bound is not UTF-8")?;
    if recovered_limit
        .parse::<u64>()
        .context("recovered latency bound must be milliseconds")?
        == 0
    {
        bail!("recovered latency bound must be positive and measured from the recovered fixture");
    }
    let reports = PathBuf::from(&arguments[4]);
    fs::create_dir(&reports)
        .context("report directory must be new; existing receipts cannot be reused")?;
    let reports = fs::canonicalize(reports)?;
    let cases = cases(recovered_limit);
    let required = cases.iter().map(|case| case.id).collect::<BTreeSet<_>>();
    if required.len() != cases.len() {
        bail!("duplicate mandatory case in full-spec catalog");
    }
    let mut completed = BTreeSet::new();
    fs::write(
        reports.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "status": "untested", "scope": "full-java-e2e", "mandatory": required,
            "recovered_handoff_max_ms": recovered_limit,
        }))?,
    )?;
    let mut signals = InvocationSignals::new()?;
    for case in &cases {
        println!(
            "Running mandatory case {} ({}/{})",
            case.id,
            completed.len() + 1,
            cases.len()
        );
        let receipt_path = reports.join(format!("{}.receipt.json", case.id));
        let log_path = reports.join(format!("{}.log", case.id));
        let nonce = format!(
            "{}-{}-{}",
            std::process::id(),
            case.id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        let slice = format!("pumpkin-e2e-{}.slice", nonce);
        let scope_journal = reports.join(format!("{}.scopes", case.id));
        let log = fs::File::create(&log_path)?;
        let mut command = Command::new(&fixture);
        for (key, _) in env::vars_os() {
            if key.to_string_lossy().starts_with("PUMPKIN_E2E_")
                || key.to_string_lossy().starts_with("PUMPKIN_CLUSTER_E2E_")
            {
                command.env_remove(key);
            }
        }
        command
            .env("PUMPKIN_E2E_PUMPKIN_BIN", &feature_server)
            .env("PUMPKIN_E2E_CASE", case.id)
            .env("PUMPKIN_E2E_NONCE", &nonce)
            .env("PUMPKIN_E2E_RECEIPT_PATH", &receipt_path)
            .env("PUMPKIN_E2E_SYSTEMD_SLICE", &slice)
            .env("PUMPKIN_E2E_SCOPE_JOURNAL", &scope_journal)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .process_group(0);
        if let Some(scenario) = case.scenario {
            command.env("PUMPKIN_E2E_SPEC_SCENARIOS", scenario);
        }
        if case.id == "normal-feature-omission" {
            command.env("PUMPKIN_E2E_NORMAL_BIN", &normal_server);
        }
        for (key, value) in &case.environment {
            command.env(key, value);
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("starting {}", case.id))?;
        let group = Pid::from_raw(child.id().context("fixture PID absent")? as i32);
        let outcome = wait_invocation(&mut child, Duration::from_secs(1020), &mut signals).await;
        let cleanup = cleanup_invocation(&mut child, group, &slice, &scope_journal).await;
        cleanup.with_context(|| format!("cleaning invocation {}", case.id))?;
        let status = outcome.with_context(|| {
            format!(
                "mandatory case {} did not finish; full suite remains unaccepted; log {}",
                case.id,
                log_path.display()
            )
        })?;
        if !status.success() {
            bail!(
                "mandatory case {} failed ({status}); full suite remains unaccepted; log {}",
                case.id,
                log_path.display()
            );
        }
        let receipt: Value =
            serde_json::from_slice(&fs::read(&receipt_path).with_context(|| {
                format!("{} exited without a completed assertion receipt", case.id)
            })?)?;
        let scenarios = case.scenario.into_iter().collect::<Vec<_>>();
        if receipt["status"] != "passed"
            || receipt["scope"] != "partial"
            || receipt["case"] != case.id
            || receipt["nonce"] != nonce
            || receipt["scenarios"] != json!(scenarios)
            || receipt["normal_feature"] != (case.id == "normal-feature-omission")
            || receipt["full_radius"] != (case.id == "full-radius-recovered-latency")
            || receipt["large_admin"] != (case.id == "large-admin-snapshot")
        {
            bail!(
                "{} receipt does not prove the requested invocation; full suite remains unaccepted",
                case.id
            );
        }
        for (key, field) in [
            ("PUMPKIN_E2E_ONE_WAY_DELAY_MS", "one_way_delay_ms"),
            (
                "PUMPKIN_E2E_RECOVERED_HANDOFF_MAX_MS",
                "recovered_handoff_max_ms",
            ),
            ("PUMPKIN_E2E_DELAY_CHUNK", "delayed_chunk"),
        ] {
            let expected = case
                .environment
                .iter()
                .find(|(candidate, _)| *candidate == key)
                .map(|(_, value)| value.as_str())
                .unwrap_or_default();
            if receipt[field] != expected {
                bail!("{} skipped or changed required {field}", case.id);
            }
        }
        if let Some(scenario) = case.scenario {
            let log = fs::read_to_string(&log_path)?;
            if log
                .lines()
                .any(|line| line.starts_with(&format!("untested {scenario}:")))
            {
                bail!(
                    "mandatory scenario {scenario} reported untested; full suite remains unaccepted"
                );
            }
        }
        completed.insert(case.id);
        fs::write(
            reports.join("progress.json"),
            serde_json::to_vec_pretty(&json!({
                "status": "incomplete", "completed": completed, "mandatory": required,
            }))?,
        )?;
    }
    if completed != required {
        bail!("mandatory scenarios were skipped or left unrun");
    }
    fs::write(
        reports.join("acceptance.json"),
        serde_json::to_vec_pretty(&json!({
            "status": "passed", "scope": "full-java-e2e", "completed": completed,
            "separate_required_gates": ["Bedrock protocol/handler coverage", "wire/property/fuzz tests", "runtime lock/wait audit"],
        }))?,
    )?;
    println!(
        "Full Java E2E suite passed all {} mandatory fresh invocations; reports {}. Separate protocol and runtime-audit gates remain required for project acceptance.",
        completed.len(),
        reports.display()
    );
    Ok(())
}

fn executable(path: &std::ffi::OsStr) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let path = fs::canonicalize(path).with_context(|| {
        format!(
            "required executable {} is missing",
            PathBuf::from(path).display()
        )
    })?;
    let metadata = fs::metadata(&path)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        bail!("required executable {} is not executable", path.display());
    }
    Ok(path)
}

struct InvocationSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl InvocationSignals {
    fn new() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }
}

async fn wait_invocation(
    child: &mut Child,
    limit: Duration,
    signals: &mut InvocationSignals,
) -> Result<std::process::ExitStatus> {
    tokio::select! {
        result = timeout(limit, child.wait()) => result.context("fixture outer deadline exceeded")?.context("waiting for fixture"),
        _ = signals.interrupt.recv() => bail!("full-spec invocation interrupted by SIGINT"),
        _ = signals.terminate.recv() => bail!("full-spec invocation interrupted by SIGTERM"),
    }
}

fn signal_group(group: Pid, signal: Signal) -> Result<()> {
    match killpg(group, signal) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn cleanup_invocation(
    child: &mut Child,
    group: Pid,
    slice: &str,
    scope_journal: &std::path::Path,
) -> Result<()> {
    let mut failure = signal_group(group, Signal::SIGTERM).err();
    if timeout(Duration::from_secs(2), child.wait()).await.is_err() {
        if let Err(error) = signal_group(group, Signal::SIGKILL) {
            failure = Some(error);
        }
        if let Err(error) = timeout(Duration::from_secs(5), child.wait())
            .await
            .context("fixture reap timed out")
            .and_then(|result| result.context("fixture reap failed"))
        {
            failure = Some(error);
        }
    }
    if let Err(error) = signal_group(group, Signal::SIGKILL) {
        failure = Some(error);
    }
    if scope_journal.exists() {
        let mut stop = Command::new("systemctl");
        stop.args(["--user", "stop", slice]).kill_on_drop(true);
        let status = timeout(Duration::from_secs(10), stop.status()).await;
        match status {
            Ok(Ok(status)) if status.success() => (),
            _ => {
                let mut kill = Command::new("systemctl");
                kill.args(["--user", "kill", "--signal=SIGKILL", slice])
                    .kill_on_drop(true);
                let _ = timeout(Duration::from_secs(5), kill.status()).await;
                let mut stop = Command::new("systemctl");
                stop.args(["--user", "stop", slice]).kill_on_drop(true);
                if !timeout(Duration::from_secs(10), stop.status())
                    .await
                    .context("scope cleanup timed out")??
                    .success()
                {
                    bail!(
                        "could not stop invocation slice {slice}; owned scopes recorded in {}",
                        scope_journal.display()
                    );
                }
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn outer_timeout_stops_and_reaps_nested_process() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let ready = directory.path().join("ready");
        let stopped = directory.path().join("stopped");
        let mut command = Command::new("bash");
        command.args(["-c", "trap 'wait; exit 0' TERM; bash -c 'trap \"printf stopped > \\\"$2\\\"; exit 0\" TERM; printf ready > \"$1\"; sleep 300 & wait' nested \"$1\" \"$2\" & wait", "fixture"])
            .arg(&ready).arg(&stopped).process_group(0).kill_on_drop(true);
        let mut signals = InvocationSignals::new()?;
        let mut child = command.spawn()?;
        let group = Pid::from_raw(child.id().context("dummy fixture PID absent")? as i32);
        let outcome = wait_invocation(&mut child, Duration::from_millis(400), &mut signals).await;
        cleanup_invocation(
            &mut child,
            group,
            "unused.slice",
            &directory.path().join("unused.scopes"),
        )
        .await?;
        if outcome.is_ok() {
            bail!("dummy nested process did not reach outer timeout");
        }
        if !ready.exists() || fs::read_to_string(stopped)? != "stopped" {
            bail!("nested process did not execute its shutdown handler");
        }
        if child.try_wait()?.is_none() {
            bail!("fixture process was not reaped");
        }
        if killpg(group, None) != Err(nix::errno::Errno::ESRCH) {
            bail!("nested process group survived cleanup");
        }
        Ok(())
    }
}
