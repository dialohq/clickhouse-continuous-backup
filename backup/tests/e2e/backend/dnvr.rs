use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use tempfile::TempDir;
use tokio::{
    fs,
    process::Command,
    time::{Instant, sleep},
};

use super::{Component, Endpoints, EnvironmentBackend};
use crate::timing::Timings;

const START_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Deserialize)]
struct ProcessList {
    processes: Vec<ProcessState>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProcessState {
    name: String,
    running: bool,
    exit_code: Option<i32>,
}

impl ProcessState {
    fn failed_startup(&self) -> bool {
        // Setup jobs may finish successfully. The servers must stay alive.
        !self.running
            && (matches!(
                self.name.as_str(),
                "clickhouse" | "redpanda" | "connect" | "minio"
            ) || self.exit_code != Some(0))
    }
}

pub struct DnvrBackend {
    instance: TempDir,
    project_root: PathBuf,
    api_url: Option<String>,
    server_pid: Option<u32>,
    client: Client,
    timings: Timings,
}

impl DnvrBackend {
    pub fn new(project_root: impl Into<PathBuf>, timings: Timings) -> Result<Self> {
        let instance = tempfile::Builder::new().prefix("durable-e2e-").tempdir()?;
        // The report owns logs separately from disposable service data, so it
        // can retain them even when startup fails and this backend is dropped.
        std::os::unix::fs::symlink(timings.logs_dir(), instance.path().join("logs"))?;
        Ok(Self {
            instance,
            project_root: project_root.into(),
            api_url: None,
            server_pid: None,
            client: Client::new(),
            timings,
        })
    }

    pub fn state_dir(&self) -> &Path {
        self.instance.path()
    }

    fn tmux_executable(&self) -> Result<PathBuf> {
        let pid = self.server_pid.context("tmux server has not started")?;
        Ok(std::fs::read_link(format!("/proc/{pid}/exe"))?)
    }

    async fn log_tail(&self, process: &str) -> String {
        let log = self
            .timings
            .logs_dir()
            .join("tmux-default-up")
            .join(format!("{process}.log"));
        let contents = fs::read_to_string(&log)
            .await
            .unwrap_or_else(|error| format!("cannot read {}: {error}", log.display()));
        contents
            .lines()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn check_processes(&self) -> Result<()> {
        let api = self
            .api_url
            .as_ref()
            .context("dnvr API has not been discovered")?;
        let list: ProcessList = self
            .client
            .get(format!("{api}/v1/processes"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .context("check startup process state through dnvr API")?
            .error_for_status()?
            .json()
            .await
            .context("decode dnvr process state")?;
        for process in list.processes {
            if process.failed_startup() {
                let exit = process
                    .exit_code
                    .map_or_else(|| "unknown".to_owned(), |code| code.to_string());
                bail!(
                    "process {} exited during startup (exit code {exit})\nLast process log lines:\n{}",
                    process.name,
                    self.log_tail(&process.name).await
                );
            }
        }
        Ok(())
    }

    async fn wait_for_process_failure(&self) -> anyhow::Error {
        loop {
            if let Err(error) = self.check_processes().await {
                return error;
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn state(&self, process: &str, key: &str, deadline: Instant) -> Result<String> {
        let path = self.instance.path().join("runtime").join(process).join(key);
        loop {
            if let Ok(value) = fs::read_to_string(&path).await {
                return Ok(value.trim().to_owned());
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for {}.{} in {}\nLast process log lines:\n{}",
                    process,
                    key,
                    path.display(),
                    self.log_tail(process).await
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn discover_api(&self, deadline: Instant) -> Result<String> {
        let socket = self.instance.path().join("runtime/tmux-default-up.sock");
        loop {
            let output = Command::new(self.tmux_executable()?)
                .arg("-S")
                .arg(&socket)
                .args(["show-option", "-gv", "@dnvr_sidebar_api_url"])
                .output()
                .await?;
            if output.status.success() {
                let url = String::from_utf8(output.stdout)?.trim().to_owned();
                if !url.is_empty() {
                    return Ok(url);
                }
            }
            if let Some(server_pid) = self.server_pid
                && let Some(url) = sidebar_api_url(server_pid)?
            {
                return Ok(url);
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out discovering the dnvr API through {}",
                    socket.display()
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    fn discover_server_pid(&self) -> Result<u32> {
        let socket = self.instance.path().join("runtime/tmux-default-up.sock");
        for entry in std::fs::read_dir("/proc")?.flatten() {
            if std::fs::read_to_string(entry.path().join("comm"))
                .unwrap_or_default()
                .trim()
                != "tmux: server"
            {
                continue;
            }
            let command = std::fs::read(entry.path().join("cmdline")).unwrap_or_default();
            if command
                .split(|byte| *byte == 0)
                .any(|arg| arg == socket.as_os_str().as_encoded_bytes())
            {
                return Ok(entry.file_name().to_string_lossy().parse()?);
            }
        }
        bail!("cannot find tmux server for {}", socket.display())
    }

    fn signal_process_tree(&self, signal: i32) {
        let runtime = self.instance.path().join("runtime");
        if let Ok(entries) = std::fs::read_dir(runtime) {
            for entry in entries.flatten() {
                let pid = std::fs::read_to_string(entry.path().join("pid"))
                    .ok()
                    .and_then(|value| value.trim().parse::<u32>().ok());
                if let Some(pid) = pid
                    && let Ok(pid) = i32::try_from(pid)
                    && pid > 1
                {
                    // Negative PID targets the process group started by tmux.
                    unsafe {
                        libc::kill(-pid, signal);
                    }
                }
            }
        }
        if let Some(pid) = self.server_pid
            && let Ok(pid) = i32::try_from(pid)
            && pid > 1
        {
            unsafe {
                libc::kill(pid, signal);
            }
        }
    }

    async fn action(&self, component: Component, action: &str) -> Result<()> {
        let api = self
            .api_url
            .as_ref()
            .context("dnvr backend is not running")?;
        self.client
            .post(format!(
                "{api}/v1/processes/{}/{action}",
                component.process_name()
            ))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[async_trait]
impl EnvironmentBackend for DnvrBackend {
    async fn start(&mut self) -> Result<Endpoints> {
        let socket = self.instance.path().join("runtime/tmux-default-up.sock");
        eprintln!(
            "[{}] logs: {}",
            self.timings.name,
            self.timings.logs_dir().join("tmux-default-up").display()
        );
        let mut child = Command::new("dnvr")
            .arg("up")
            .current_dir(&self.project_root)
            .env("DNVR_STATE", self.instance.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("start dnvr; run the test from `nix develop`")?;

        // `dnvr up` creates a detached tmux server, then its attempted attach
        // exits because this test does not provide a terminal.
        self.timings
            .measure("launch dnvr", async {
                child.wait().await.map_err(Into::into)
            })
            .await?;
        // Record the server before waiting for services, so failed startup
        // still cleans up the tmux session even if ClickHouse already exited.
        self.server_pid = Some(self.discover_server_pid()?);
        // Use the server's own executable: a system tmux client may be a
        // different version from the one dnvr obtained through Nix.
        eprintln!(
            "[{}] watch: '{}' -S '{}' attach-session -t dnvr",
            self.timings.name,
            self.tmux_executable()?
                .display()
                .to_string()
                .replace('\'', "'\\''"),
            socket.display().to_string().replace('\'', "'\\''")
        );

        let deadline = Instant::now() + START_TIMEOUT;
        self.api_url = Some(
            self.timings
                .measure("discover dnvr API", self.discover_api(deadline))
                .await?,
        );
        self.wait_for_startup(deadline).await
    }

    async fn restart(&self, component: Component) -> Result<()> {
        self.action(component, "restart?wipeState=false").await
    }

    async fn interrupt(&self, component: Component) -> Result<()> {
        self.action(component, "interrupt").await
    }

    async fn logs(&self, component: Component) -> Result<String> {
        let output = Command::new("dnvr")
            .args(["logs", component.process_name()])
            .current_dir(&self.project_root)
            .env("DNVR_STATE", self.instance.path())
            .output()
            .await?;
        if !output.status.success() {
            bail!(
                "dnvr logs failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8(output.stdout)?)
    }

    async fn stop(&mut self) -> Result<()> {
        let Some(api) = self.api_url.take() else {
            return Ok(());
        };
        let result = self
            .client
            .post(format!("{api}/v1/stop"))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map(|_| ());
        self.signal_process_tree(libc::SIGTERM);
        sleep(Duration::from_secs(2)).await;
        self.signal_process_tree(libc::SIGKILL);
        self.server_pid = None;
        result.map_err(Into::into)
    }
}

impl DnvrBackend {
    async fn wait_for_startup(&self, deadline: Instant) -> Result<Endpoints> {
        tokio::select! {
            error = self.wait_for_process_failure() => Err(error),
            result = self.wait_for_endpoints(deadline) => {
                let endpoints = result?;
                // Check once more even if every readiness file was already present.
                self.check_processes().await?;
                Ok(endpoints)
            }
        }
    }

    async fn wait_for_endpoints(&self, deadline: Instant) -> Result<Endpoints> {
        // Measure all service waits from the same point; sequential waits
        // would hide time ClickHouse spends starting while Redpanda starts.
        let (kafka, clickhouse_host, connect_url) = tokio::try_join!(
            self.timings.measure(
                "waiting for Redpanda readiness",
                self.state("redpanda", "bootstrapServers", deadline)
            ),
            self.timings.measure(
                "waiting for ClickHouse readiness",
                self.state("clickhouse", "host", deadline)
            ),
            self.timings.measure(
                "waiting for Connect REST readiness",
                self.state("connect", "url", deadline)
            ),
        )?;
        let clickhouse_tcp_port = self
            .state("clickhouse", "tcpPort", deadline)
            .await?
            .parse()?;
        let clickhouse_http_url = self.state("clickhouse", "httpUrl", deadline).await?;

        Ok(Endpoints {
            kafka,
            clickhouse_host,
            clickhouse_tcp_port,
            clickhouse_http_url,
            connect_url,
        })
    }
}

impl Drop for DnvrBackend {
    fn drop(&mut self) {
        self.signal_process_tree(libc::SIGTERM);
        self.signal_process_tree(libc::SIGKILL);
    }
}

fn sidebar_api_url(server_pid: u32) -> Result<Option<String>> {
    let sidebar_pid = std::fs::read_dir("/proc")?
        .filter_map(Result::ok)
        .find_map(|entry| {
            let pid = entry.file_name().to_string_lossy().parse::<u32>().ok()?;
            let status = std::fs::read_to_string(entry.path().join("status")).ok()?;
            let parent = status
                .lines()
                .find_map(|line| line.strip_prefix("PPid:")?.trim().parse::<u32>().ok())?;
            let name = status
                .lines()
                .find_map(|line| line.strip_prefix("Name:\t"))?;
            (parent == server_pid && name.starts_with("dnvr-tmux-sideb")).then_some(pid)
        });
    let Some(sidebar_pid) = sidebar_pid else {
        return Ok(None);
    };

    let sockets: Vec<_> = std::fs::read_dir(format!("/proc/{sidebar_pid}/fd"))?
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter_map(|target| {
            let target = target.to_string_lossy();
            target
                .strip_prefix("socket:[")?
                .strip_suffix(']')?
                .parse::<u64>()
                .ok()
        })
        .collect();
    let tcp = std::fs::read_to_string(format!("/proc/{sidebar_pid}/net/tcp"))?;
    for line in tcp.lines().skip(1) {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 10 || fields[3] != "0A" {
            continue;
        }
        let Some((address, port)) = fields[1].split_once(':') else {
            continue;
        };
        let Ok(inode) = fields[9].parse::<u64>() else {
            continue;
        };
        if address == "0100007F" && sockets.contains(&inode) {
            return Ok(Some(format!(
                "http://127.0.0.1:{}",
                u16::from_str_radix(port, 16)?
            )));
        }
    }
    Ok(None)
}
