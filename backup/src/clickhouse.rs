use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::de::DeserializeOwned;

use crate::{
    config::RuntimeTimeouts,
    model::{BackupDetails, KeeperRow, Pipeline},
};

#[derive(Debug, serde::Deserialize)]
struct TableEngine {
    database: String,
    name: String,
    engine: String,
    create_table_query: String,
}

#[derive(Debug, serde::Deserialize)]
struct SettingValue {
    value: String,
}

#[derive(Clone)]
pub struct ClickHouse {
    client: Client,
    url: String,
    username: String,
    password: String,
}

impl ClickHouse {
    pub fn new(
        url: String,
        username: String,
        password: String,
        timeouts: &RuntimeTimeouts,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .connect_timeout(timeouts.clickhouse_connect)
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

    pub async fn upload_backup(
        &self,
        objects: &str,
        destination: &str,
        base: Option<&str>,
        max_bandwidth: u64,
    ) -> Result<(uuid::Uuid, String)> {
        let mut settings = Vec::new();
        if let Some(base) = base {
            settings.push(format!("base_backup = {base}"));
        }
        if max_bandwidth > 0 {
            settings.push(format!("max_backup_bandwidth = {max_bandwidth}"));
        }
        let settings = if settings.is_empty() {
            String::new()
        } else {
            format!(" SETTINGS {}", settings.join(", "))
        };
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

    pub async fn require_backup_created(
        &self,
        id: uuid::Uuid,
        destination: &str,
    ) -> Result<BackupDetails> {
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

    pub async fn clone_target(&self, database: &str, source: &str, snapshot: &str) -> Result<()> {
        self.query(&format!(
            "CREATE TABLE `{database}`.`{snapshot}` ENGINE = MergeTree CLONE AS `{database}`.`{source}`"
        ))
        .await?;
        Ok(())
    }

    pub async fn drop_table(&self, database: &str, table: &str) -> Result<()> {
        self.query(&format!("DROP TABLE IF EXISTS `{database}`.`{table}` SYNC"))
            .await?;
        Ok(())
    }

    pub async fn require_backup_engines(&self, pipelines: &[Pipeline]) -> Result<()> {
        self.require_engines(pipelines, true).await
    }

    pub async fn require_target_engines(&self, pipelines: &[Pipeline]) -> Result<()> {
        self.require_engines(pipelines, false).await
    }

    async fn require_engines(&self, pipelines: &[Pipeline], require_state: bool) -> Result<()> {
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
                "SELECT database, name, engine, create_table_query FROM system.tables WHERE (database, name) IN ({names}) FORMAT JSONEachRow"
            ))
            .await?;
        let defaults: Vec<SettingValue> = self
            .json_each_row(
                "SELECT value FROM system.merge_tree_settings WHERE name = 'replicated_deduplication_window' FORMAT JSONEachRow",
            )
            .await?;
        let [default] = defaults.as_slice() else {
            bail!("ClickHouse did not return replicated_deduplication_window")
        };
        let replicated_default = default
            .value
            .parse()
            .context("ClickHouse returned an invalid replicated_deduplication_window")?;
        validate_engines(pipelines, &engines, replicated_default, require_state)
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
    replicated_default: u64,
    require_state: bool,
) -> Result<()> {
    for pipeline in pipelines {
        let database = &pipeline.database;
        let table = &pipeline.table;
        let engine = engines
            .iter()
            .find(|engine| engine.database == database.as_str() && engine.name == table.as_str())
            .with_context(|| format!("backup table does not exist: {database}.{table}"))?;
        if !engine.engine.ends_with("MergeTree") {
            bail!(
                "exactly-once delivery requires {database}.{table} to use MergeTree, found {}",
                engine.engine
            )
        }
        let replicated = engine.engine.contains("Replicated") || engine.engine == "SharedMergeTree";
        if replicated {
            if table_setting(
                &engine.create_table_query,
                "replicated_deduplication_window",
            )
            .unwrap_or(replicated_default)
                == 0
            {
                bail!("{database}.{table} disables replicated insert deduplication")
            }
        } else if table_setting(
            &engine.create_table_query,
            "non_replicated_deduplication_window",
        )
        .is_none_or(|window| window == 0)
        {
            bail!(
                "non-replicated {database}.{table} must explicitly set non_replicated_deduplication_window to a positive value"
            )
        }
        if require_state {
            let state_table = &pipeline.state_table;
            let state = engines
                .iter()
                .find(|engine| {
                    engine.database == database.as_str() && engine.name == state_table.as_str()
                })
                .with_context(|| {
                    format!("backup table does not exist: {database}.{state_table}")
                })?;
            if state.engine != "KeeperMap" {
                bail!(
                    "backup manifests require {database}.{state_table} to use KeeperMap, found {}",
                    state.engine
                )
            }
        }
    }
    Ok(())
}

fn table_setting(query: &str, name: &str) -> Option<u64> {
    let (_, suffix) = query.split_once(name)?;
    let suffix = suffix.trim_start().strip_prefix('=')?.trim_start();
    suffix
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline() -> Pipeline {
        Pipeline {
            connector: "records".to_owned(),
            database: "history".to_owned(),
            state_table: "records_state".to_owned(),
            keeper_path: "/durable-clickhouse-sink/default/records".to_owned(),
            table: "records".to_owned(),
            topic: "records.input".to_owned(),
            partitions: 3,
        }
    }

    fn engine(name: &str, value: &str) -> TableEngine {
        TableEngine {
            database: "history".to_owned(),
            name: name.to_owned(),
            engine: value.to_owned(),
            create_table_query: if value.contains("Replicated") {
                format!("CREATE TABLE history.{name} ENGINE = {value}")
            } else {
                format!(
                    "CREATE TABLE history.{name} ENGINE = {value} SETTINGS non_replicated_deduplication_window = 1000"
                )
            },
        }
    }

    #[test]
    fn accepts_merge_tree_family_and_keeper_map() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[
                    engine("records", "ReplicatedMergeTree"),
                    engine("records_state", "KeeperMap")
                ],
                1000,
                true,
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_non_merge_tree_missing_deduplication_and_missing_state() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[
                    engine("records", "Log"),
                    engine("records_state", "KeeperMap")
                ],
                1000,
                true,
            )
            .is_err()
        );
        assert!(
            validate_engines(&[pipeline()], &[engine("records", "MergeTree")], 1000, true).is_err()
        );
        let mut unsafe_merge_tree = engine("records", "MergeTree");
        unsafe_merge_tree.create_table_query =
            "CREATE TABLE history.records ENGINE = MergeTree".to_owned();
        assert!(
            validate_engines(
                &[pipeline()],
                &[unsafe_merge_tree, engine("records_state", "KeeperMap")],
                1000,
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn full_backups_also_require_safe_target_and_keeper_map() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[
                    engine("records", "MergeTree"),
                    engine("records_state", "Log")
                ],
                1000,
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_disabled_replicated_default() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[
                    engine("records", "ReplicatedMergeTree"),
                    engine("records_state", "KeeperMap")
                ],
                0,
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn target_preflight_does_not_require_connector_state_yet() {
        assert!(
            validate_engines(
                &[pipeline()],
                &[engine("records", "ReplicatedMergeTree")],
                1000,
                false,
            )
            .is_ok()
        );
    }
}
