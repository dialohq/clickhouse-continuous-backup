use std::env;

use anyhow::{Context, Result, bail};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, ResourceExt,
    api::{ListParams, Patch, PatchParams},
};
use serde_json::json;
use tracing::info;

pub(super) const ROLE_LABEL: &str = "durable-clickhouse.dialo.ai/pause-server-role";
const ACTIVE: &str = "active";
const POD_SELECTOR_LABELS: [&str; 3] = [
    "app.kubernetes.io/name",
    "app.kubernetes.io/instance",
    "app.kubernetes.io/component",
];

#[derive(Clone)]
pub(super) struct PodLabels {
    pods: Api<Pod>,
    pod: String,
}

impl PodLabels {
    pub(super) async fn from_env() -> Result<Option<Self>> {
        let Ok(pod) = env::var("POD_NAME") else {
            return Ok(None);
        };
        let namespace =
            env::var("POD_NAMESPACE").context("POD_NAMESPACE is required with POD_NAME")?;
        let client = Client::try_default()
            .await
            .context("failed to create the Kubernetes client")?;
        Ok(Some(Self {
            pods: Api::namespaced(client, &namespace),
            pod,
        }))
    }

    pub(super) fn pod(&self) -> &str {
        &self.pod
    }

    pub(super) async fn claim(&self) -> Result<()> {
        let own = self
            .pods
            .get(&self.pod)
            .await
            .with_context(|| format!("failed to get pod {}", self.pod))?;
        let labels = own.labels();
        let mut selector = Vec::new();
        for key in POD_SELECTOR_LABELS {
            let Some(value) = labels.get(key) else {
                bail!("pod {} has no {key} label", self.pod);
            };
            selector.push(format!("{key}={value}"));
        }
        selector.push(format!("{ROLE_LABEL}={ACTIVE}"));
        let labelled = self
            .pods
            .list(&ListParams::default().labels(&selector.join(",")))
            .await
            .context("failed to list the pause server pods")?;
        for pod in labelled {
            let name = pod.name_any();
            if name != self.pod {
                self.set_role(&name, None).await?;
                info!(pod = name, "removed the active label from a former leader");
            }
        }
        if labels.get(ROLE_LABEL).map(String::as_str) != Some(ACTIVE) {
            self.set_role(&self.pod, Some(ACTIVE)).await?;
            info!(pod = self.pod, "labelled this pod active");
        }
        Ok(())
    }

    pub(super) async fn release(&self) -> Result<()> {
        self.set_role(&self.pod, None).await
    }

    async fn set_role(&self, pod: &str, role: Option<&str>) -> Result<()> {
        self.pods
            .patch(
                pod,
                &PatchParams::default(),
                &Patch::Merge(json!({"metadata": {"labels": {ROLE_LABEL: role}}})),
            )
            .await
            .with_context(|| format!("failed to label pod {pod}"))?;
        Ok(())
    }
}
