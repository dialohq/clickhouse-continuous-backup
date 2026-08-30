use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::de::DeserializeOwned;

use crate::model::{BackupDetails, KeeperRow, Pipeline};

#[derive(Debug, serde::Deserialize)]
struct TableEngine {
    database: String,
    name: String,
    engine: String,
}

#[derive(Clone)]
pub struct ClickHouse {
    client: Client,
    url: String,
    username: String,
    password: String,
}

impl ClickHouse {
    pub fn new(url: String, username: String, password: String) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            url,
            username,
            password,
        })
    }

    pub async fn query(&self, sql: &str) -> Result<String> {
        let response = self
            .client
            .post(&self.url)
            .basic_auth(&self.username, Some(&self.password))
            .body(sql.to_owned())
            .send()
            .await?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read ClickHouse response")?;
        if !status.is_success() {
            bail!("ClickHouse returned {status}: {}", body.trim_end())
        }
        Ok(body)
    }

    pub async fn create_backup(
        &self,
        objects: &str,
        destination: &str,
        base: Option<&str>,
    ) -> Result<(uuid::Uuid, String)> {
        let settings = base.map_or(String::new(), |base| {
            format!(" SETTINGS base_backup = {base}")
        });
        let response = self
            .query(&format!("BACKUP {objects} TO {destination}{settings}"))
            .await?;
        let fields = response.trim_end().split('\t').collect::<Vec<_>>();
        if fields.len() != 2 || fields[1] != "BACKUP_CREATED" {
            bail!(
                "unexpected ClickHouse BACKUP result: {}",
                response.trim_end()
            )
        }
        Ok((
            fields[0]
                .parse()
                .context("ClickHouse returned an invalid backup ID")?,
            fields[1].to_owned(),
        ))
    }

    pub async fn backup_details(&self, id: uuid::Uuid, destination: &str) -> Result<BackupDetails> {
        let rows: Vec<BackupDetails> = self
            .json_each_row(&format!(
                "SELECT id, name, status, num_files, uncompressed_size, compressed_size FROM system.backups WHERE id = '{id}' FORMAT JSONEachRow"
            ))
            .await?;
        let [details] = rows.as_slice() else {
            bail!("ClickHouse did not return exactly one backup status row: {id}")
        };
        if details.name != destination || details.status != "BACKUP_CREATED" {
            bail!("ClickHouse did not confirm the created backup: {id}")
        }
        Ok(details.clone())
    }

    pub async fn keeper_rows(&self, database: &str, table: &str) -> Result<Vec<KeeperRow>> {
        self.json_each_row(&format!(
            "SELECT key, minOffset, maxOffset, state FROM `{database}`.`{table}` FORMAT JSONEachRow"
        ))
        .await
    }

    pub async fn require_backup_engines(
        &self,
        pipelines: &[Pipeline],
        incremental: bool,
    ) -> Result<()> {
        let names = pipelines
            .iter()
            .flat_map(|pipeline| {
                [
                    (&pipeline.database, &pipeline.table),
                    (&pipeline.database, &pipeline.state_table),
                ]
            })
            .map(|(database, table)| format!("('{database}', '{table}')"))
            .collect::<Vec<_>>()
            .join(", ");
        let engines: Vec<TableEngine> = self
            .json_each_row(&format!(
                "SELECT database, name, engine FROM system.tables WHERE (database, name) IN ({names}) FORMAT JSONEachRow"
            ))
            .await?;
        validate_engines(pipelines, &engines, incremental)
    }

    async fn json_each_row<T: DeserializeOwned>(&self, sql: &str) -> Result<Vec<T>> {
        self.query(sql)
            .await?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_str(line).context("ClickHouse returned invalid JSONEachRow")
            })
            .collect()
    }
}

fn validate_engines(
    pipelines: &[Pipeline],
    engines: &[TableEngine],
    incremental: bool,
) -> Result<()> {
    for pipeline in pipelines {
        let database = &pipeline.database;
        let table = &pipeline.table;
        let engine = engines
            .iter()
            .find(|engine| engine.database == database.as_str() && engine.name == table.as_str())
            .with_context(|| format!("backup table does not exist: {database}.{table}"))?;
        if incremental && !engine.engine.ends_with("MergeTree") {
            bail!(
                "incremental backups require {database}.{table} to use MergeTree, found {}",
                engine.engine
            )
        }
        let state_table = &pipeline.state_table;
        let state = engines
            .iter()
            .find(|engine| {
                engine.database == database.as_str() && engine.name == state_table.as_str()
            })
            .with_context(|| format!("backup table does not exist: {database}.{state_table}"))?;
        if state.engine != "KeeperMap" {
            bail!(
                "recovery checkpoints require {database}.{state_table} to use KeeperMap, found {}",
                state.engine
            )
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline() -> Pipeline {
        Pipeline {
            connector: "events".to_owned(),
            database: "history".to_owned(),
            state_table: "events_state".to_owned(),
            table: "events".to_owned(),
            topic: "events.canonical".to_owned(),
            partitions: 3,
        }
    }

    fn engine(name: &str, value: &str) -> TableEngine {
        TableEngine {
            database: "history".to_owned(),
            name: name.to_owned(),
            engine: value.to_owned(),
        }
    }

    #[test]
    fn accepts_merge_tree_family_and_keeper_map() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[
                    engine("events", "ReplicatedMergeTree"),
                    engine("events_state", "KeeperMap")
                ],
                true,
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_append_only_engine_and_missing_state() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[engine("events", "Log"), engine("events_state", "KeeperMap")],
                true,
            )
            .is_err()
        );
        assert!(validate_engines(&[pipeline()], &[engine("events", "MergeTree")], true).is_err());
    }

    #[test]
    fn full_backups_allow_other_target_engines_but_require_keeper_map() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[engine("events", "Log"), engine("events_state", "KeeperMap")],
                false,
            )
            .is_ok()
        );
        assert!(
            validate_engines(
                &[pipeline()],
                &[engine("events", "MergeTree"), engine("events_state", "Log")],
                false,
            )
            .is_err()
        );
    }
}
