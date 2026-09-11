use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tempfile::TempDir;

type Step = (String, Duration, &'static str);

#[derive(Clone)]
pub struct Timings {
    pub name: &'static str,
    steps: Arc<Mutex<Vec<Step>>>,
    logs_dir: PathBuf,
}

pub struct TestReport {
    timings: Timings,
    started: Instant,
    artifacts: Option<TempDir>,
    successful: bool,
}

impl TestReport {
    pub fn new(name: &'static str) -> Result<Self> {
        Self::in_dir(
            name,
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/e2e-logs"),
        )
    }

    fn in_dir(name: &'static str, root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root).context("create E2E artifact directory")?;
        let artifacts = tempfile::Builder::new()
            .prefix(&format!("{name}-"))
            .tempdir_in(root)?;
        let logs_dir = artifacts.path().join("logs");
        std::fs::create_dir(&logs_dir)?;
        eprintln!("[{name}] test started");
        Ok(Self {
            timings: Timings {
                name,
                steps: Arc::default(),
                logs_dir,
            },
            started: Instant::now(),
            artifacts: Some(artifacts),
            successful: false,
        })
    }

    pub fn mark_success(&mut self) {
        self.successful = true;
    }

    pub fn timings(&self) -> Timings {
        self.timings.clone()
    }
}

impl Timings {
    pub fn logs_dir(&self) -> &Path {
        &self.logs_dir
    }

    pub async fn measure<T>(
        &self,
        label: impl Into<String>,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let label = label.into();
        eprintln!("[{}] {label}: started", self.name);
        let started = Instant::now();
        let result = operation.await;
        let elapsed = started.elapsed();
        let status = if result.is_ok() { "ok" } else { "error" };
        eprintln!(
            "[{}] {label}: {:.2}s ({status})",
            self.name,
            elapsed.as_secs_f64()
        );
        self.steps.lock().unwrap().push((label, elapsed, status));
        result
    }
}

impl Drop for TestReport {
    fn drop(&mut self) {
        let mut summary = format!(
            "[{}] total: {:.2}s\n",
            self.timings.name,
            self.started.elapsed().as_secs_f64()
        );
        for (label, elapsed, status) in self
            .timings
            .steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            summary.push_str(&format!(
                "  {label}: {:.2}s ({status})\n",
                elapsed.as_secs_f64()
            ));
        }
        if (!self.successful || std::thread::panicking())
            && let Some(artifacts) = self.artifacts.take()
        {
            let path = artifacts.keep();
            if let Err(error) = std::fs::write(path.join("timings.txt"), &summary) {
                eprintln!("[{}] cannot save timings: {error}", self.timings.name);
            }
            summary.push_str(&format!(
                "  failure artifacts preserved: {}\n",
                path.display()
            ));
        }
        // One print keeps summaries together when several tests finish at once.
        eprintln!("{summary}");
    }
}
