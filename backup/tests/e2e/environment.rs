use anyhow::Result;

use crate::{
    EnvironmentBackend,
    backend::{Component, Endpoints},
    clients::{BackupClient, ClickHouseClient, ConnectClient, KafkaClient},
};

pub struct TestEnvironment<B: EnvironmentBackend> {
    backend: B,
    pub endpoints: Endpoints,
    pub kafka: KafkaClient,
    pub clickhouse: ClickHouseClient,
    pub connect: ConnectClient,
    pub backup: BackupClient,
}

impl<B: EnvironmentBackend> TestEnvironment<B> {
    pub async fn start(mut backend: B) -> Result<Self> {
        let endpoints = backend.start().await?;
        let kafka = KafkaClient::new(endpoints.kafka.clone())?;
        let clickhouse = ClickHouseClient::new(endpoints.clickhouse_http_url.clone())?;
        let connect = ConnectClient::new(endpoints.connect_url.clone());
        let backup = BackupClient::new(
            endpoints.connect_url.clone(),
            endpoints.clickhouse_http_url.clone(),
            endpoints.kafka.clone(),
        );
        Ok(Self {
            backend,
            endpoints,
            kafka,
            clickhouse,
            connect,
            backup,
        })
    }

    pub async fn restart(&self, component: Component) -> Result<()> {
        self.backend.restart(component).await
    }

    pub async fn interrupt(&self, component: Component) -> Result<()> {
        self.backend.interrupt(component).await
    }

    pub async fn stop(mut self) -> Result<()> {
        self.backend.stop().await
    }
}
