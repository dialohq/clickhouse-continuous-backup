mod dnvr;

use anyhow::Result;
use async_trait::async_trait;

pub use dnvr::DnvrBackend;

#[derive(Clone, Debug)]
pub struct Endpoints {
    pub kafka: String,
    pub clickhouse_host: String,
    pub clickhouse_tcp_port: u16,
    pub clickhouse_http_url: String,
    pub connect_url: String,
}

#[derive(Clone, Copy, Debug)]
pub enum Component {
    ClickHouse,
    Connect,
    Minio,
    Redpanda,
}

impl Component {
    pub(crate) fn process_name(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::Connect => "connect",
            Self::Minio => "minio",
            Self::Redpanda => "redpanda",
        }
    }
}

#[async_trait]
pub trait EnvironmentBackend: Send {
    async fn start(&mut self) -> Result<Endpoints>;
    async fn restart(&self, component: Component) -> Result<()>;
    async fn interrupt(&self, component: Component) -> Result<()>;
    async fn logs(&self, component: Component) -> Result<String>;
    async fn stop(&mut self) -> Result<()>;
}
