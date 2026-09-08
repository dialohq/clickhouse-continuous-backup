use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::de::DeserializeOwned;

use crate::{
    config::RuntimeTimeouts,
    model::{BackupDetails, KeeperRow, Pipeline},
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(from = "String")]
enum Engine {
    MergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree,
    VersionedCollapsingMergeTree,
    GraphiteMergeTree,
    CoalescingMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree,
    ReplicatedSummingMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedCollapsingMergeTree,
    ReplicatedVersionedCollapsingMergeTree,
    ReplicatedGraphiteMergeTree,
    ReplicatedCoalescingMergeTree,
    SharedMergeTree,
    SharedReplacingMergeTree,
    SharedSummingMergeTree,
    SharedAggregatingMergeTree,
    SharedCollapsingMergeTree,
    SharedVersionedCollapsingMergeTree,
    SharedGraphiteMergeTree,
    SharedCoalescingMergeTree,
    KeeperMap,
    Other(String),
}

impl From<String> for Engine {
    fn from(name: String) -> Self {
        match name.as_str() {
            "MergeTree" => Self::MergeTree,
            "ReplacingMergeTree" => Self::ReplacingMergeTree,
            "SummingMergeTree" => Self::SummingMergeTree,
            "AggregatingMergeTree" => Self::AggregatingMergeTree,
            "CollapsingMergeTree" => Self::CollapsingMergeTree,
            "VersionedCollapsingMergeTree" => Self::VersionedCollapsingMergeTree,
            "GraphiteMergeTree" => Self::GraphiteMergeTree,
            "CoalescingMergeTree" => Self::CoalescingMergeTree,
            "ReplicatedMergeTree" => Self::ReplicatedMergeTree,
            "ReplicatedReplacingMergeTree" => Self::ReplicatedReplacingMergeTree,
            "ReplicatedSummingMergeTree" => Self::ReplicatedSummingMergeTree,
            "ReplicatedAggregatingMergeTree" => Self::ReplicatedAggregatingMergeTree,
            "ReplicatedCollapsingMergeTree" => Self::ReplicatedCollapsingMergeTree,
            "ReplicatedVersionedCollapsingMergeTree" => {
                Self::ReplicatedVersionedCollapsingMergeTree
            }
            "ReplicatedGraphiteMergeTree" => Self::ReplicatedGraphiteMergeTree,
            "ReplicatedCoalescingMergeTree" => Self::ReplicatedCoalescingMergeTree,
            "SharedMergeTree" => Self::SharedMergeTree,
            "SharedReplacingMergeTree" => Self::SharedReplacingMergeTree,
            "SharedSummingMergeTree" => Self::SharedSummingMergeTree,
            "SharedAggregatingMergeTree" => Self::SharedAggregatingMergeTree,
            "SharedCollapsingMergeTree" => Self::SharedCollapsingMergeTree,
            "SharedVersionedCollapsingMergeTree" => Self::SharedVersionedCollapsingMergeTree,
            "SharedGraphiteMergeTree" => Self::SharedGraphiteMergeTree,
            "SharedCoalescingMergeTree" => Self::SharedCoalescingMergeTree,
            "KeeperMap" => Self::KeeperMap,
            _ => Self::Other(name),
        }
    }
}

impl Engine {
    fn supported() -> Vec<Self> {
        vec![
            Self::MergeTree,
            Self::ReplacingMergeTree,
            Self::SummingMergeTree,
            Self::AggregatingMergeTree,
            Self::CollapsingMergeTree,
            Self::VersionedCollapsingMergeTree,
            Self::GraphiteMergeTree,
            Self::CoalescingMergeTree,
            Self::ReplicatedMergeTree,
            Self::ReplicatedReplacingMergeTree,
            Self::ReplicatedSummingMergeTree,
            Self::ReplicatedAggregatingMergeTree,
            Self::ReplicatedCollapsingMergeTree,
            Self::ReplicatedVersionedCollapsingMergeTree,
            Self::ReplicatedGraphiteMergeTree,
            Self::ReplicatedCoalescingMergeTree,
            Self::SharedMergeTree,
            Self::SharedReplacingMergeTree,
            Self::SharedSummingMergeTree,
            Self::SharedAggregatingMergeTree,
            Self::SharedCollapsingMergeTree,
            Self::SharedVersionedCollapsingMergeTree,
            Self::SharedGraphiteMergeTree,
            Self::SharedCoalescingMergeTree,
        ]
    }

    fn is_supported(&self) -> bool {
        Self::supported().contains(self)
    }

    // the issue here is that when creating a table without an explicit setting so it
    // inherits the default, and then changing that default in the system table to another value
    // won't show up in the create query generated for us by clickhouse. so these checks
    // are best effort but there seems to be no way to get a setting for a table for some reason.
    // but these values are not tied to creation inherited values in tables can change on restart
    // or possibly other situations, not documented well...
    fn dedup_setting(&self) -> (&'static str, &'static str) {
        if self.is_replicated() {
            (
                "system.replicated_merge_tree_settings",
                "replicated_deduplication_window",
            )
        } else {
            (
                "system.merge_tree_settings",
                "non_replicated_deduplication_window",
            )
        }
    }

    fn is_replicated(&self) -> bool {
        matches!(
            self,
            Self::ReplicatedMergeTree
                | Self::ReplicatedReplacingMergeTree
                | Self::ReplicatedSummingMergeTree
                | Self::ReplicatedAggregatingMergeTree
                | Self::ReplicatedCollapsingMergeTree
                | Self::ReplicatedVersionedCollapsingMergeTree
                | Self::ReplicatedGraphiteMergeTree
                | Self::ReplicatedCoalescingMergeTree
                // in docs they use replicated_deduplication_window setting
                // so we consider it replicated. This could fail if SharedMergeTree
                // turns out not to use the `system.replicated_merge_tree_settings` table
                // https://clickhouse.com/docs/products/cloud/features/infrastructure/shared-merge-tree#introspection
                | Self::SharedMergeTree
                | Self::SharedReplacingMergeTree
                | Self::SharedSummingMergeTree
                | Self::SharedAggregatingMergeTree
                | Self::SharedCollapsingMergeTree
                | Self::SharedVersionedCollapsingMergeTree
                | Self::SharedGraphiteMergeTree
                | Self::SharedCoalescingMergeTree
        )
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(name) => f.write_str(name),
            engine => write!(f, "{engine:?}"),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct TableEngine {
    database: String,
    name: String,
    engine: Engine,
    create_table_query: String,
}

impl TableEngine {
    async fn validate(&self, clickhouse: &ClickHouse) -> Result<()> {
        self.require_supported()?;
        let (_, setting) = self.engine.dedup_setting();
        let default = match table_setting(&self.create_table_query, setting) {
            Some(window) => window,
            None => clickhouse.default_dedup(&self.engine).await?,
        };
        self.validate_with_default(default)
    }

    fn require_supported(&self) -> Result<()> {
        let database = &self.database;
        let table = &self.name;
        if !self.engine.is_supported() {
            bail!(
                "exactly-once delivery requires {database}.{table} to use MergeTree, found {}",
                self.engine
            )
        }
        Ok(())
    }

    fn validate_with_default(&self, default: u64) -> Result<()> {
        self.require_supported()?;
        let (_, setting) = self.engine.dedup_setting();
        let window = table_setting(&self.create_table_query, setting).unwrap_or(default);
        if window == 0 {
            let database = &self.database;
            let table = &self.name;
            bail!("Invalid table insert deduplication settings for table: {database}.{table}");
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
struct RestoreOperation {
    name: String,
    status: String,
    error: String,
}

pub enum RestoreState {
    Missing,
    Running,
    Restored,
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
        parent: Option<&str>,
        max_bandwidth: u64,
    ) -> Result<(uuid::Uuid, String)> {
        let mut settings = Vec::new();
        if let Some(parent) = parent {
            settings.push(format!("base_backup = {parent}"));
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

    pub async fn ensure_keeper_table(&self, database: &str, table: &str, path: &str) -> Result<()> {
        let existing: Vec<TableEngine> = self
            .json_each_row(&format!(
                "SELECT database, name, engine, create_table_query FROM system.tables WHERE database = '{database}' AND name = '{table}' FORMAT JSONEachRow"
            ))
            .await?;
        match existing.as_slice() {
            [] => {
                self.query(&format!(
                    "CREATE TABLE `{database}`.`{table}` (`key` String, `minOffset` Int64, `maxOffset` Int64, `state` String) ENGINE = KeeperMap('{path}') PRIMARY KEY `key`"
                ))
                .await?;
            }
            [existing]
                if existing.engine == Engine::KeeperMap
                    && existing
                        .create_table_query
                        .contains(&format!("KeeperMap('{path}')")) => {}
            [existing] => bail!(
                "recovery state table has an incompatible engine or Keeper path: {database}.{table} ({})",
                existing.engine
            ),
            _ => bail!("ClickHouse returned duplicate state tables: {database}.{table}"),
        }
        Ok(())
    }

    pub async fn insert_keeper_rows(
        &self,
        database: &str,
        table: &str,
        rows: &[KeeperRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut query = format!("INSERT INTO `{database}`.`{table}` FORMAT JSONEachRow\n");
        for row in rows {
            query.push_str(&serde_json::to_string(row)?);
            query.push('\n');
        }
        self.query(&query).await?;
        Ok(())
    }

    pub async fn destination_rows(&self, database: &str, table: &str) -> Result<u64> {
        let engines: Vec<TableEngine> = self
            .json_each_row(&format!(
                "SELECT database, name, engine, create_table_query FROM system.tables WHERE database = '{database}' AND name = '{table}' FORMAT JSONEachRow"
            ))
            .await?;
        let [engine] = engines.as_slice() else {
            bail!("recovery destination must exist exactly once: {database}.{table}")
        };
        if !engine.engine.is_supported() {
            bail!("recovery destination must use a MergeTree-family engine: {database}.{table}")
        }
        self.query(&format!("SELECT count() FROM `{database}`.`{table}`"))
            .await?
            .trim()
            .parse()
            .context("ClickHouse returned an invalid destination row count")
    }

    pub async fn restore_table(
        &self,
        source_database: &str,
        source_table: &str,
        destination_database: &str,
        destination_table: &str,
        backup: &str,
        operation_id: &str,
    ) -> Result<()> {
        let response = self
            .query(&format!(
                "RESTORE TABLE `{source_database}`.`{source_table}` AS `{destination_database}`.`{destination_table}` FROM {backup} SETTINGS allow_different_table_def = true, id = '{operation_id}'"
            ))
            .await?;
        let fields = response.trim_end().split('\t').collect::<Vec<_>>();
        if fields.len() != 2 || fields[0] != operation_id || fields[1] != "RESTORED" {
            bail!(
                "unexpected ClickHouse RESTORE result: {}",
                response.trim_end()
            )
        }
        Ok(())
    }

    pub async fn restore_state(&self, operation_id: &str, backup: &str) -> Result<RestoreState> {
        let operations: Vec<RestoreOperation> = self
            .json_each_row(&format!(
                "SELECT name, status, error FROM system.backups WHERE id = '{operation_id}' FORMAT JSONEachRow"
            ))
            .await?;
        let Some(operation) = operations.first() else {
            return Ok(RestoreState::Missing);
        };
        if operations.len() != 1 || operation.name != backup {
            bail!("ClickHouse restore operation identity conflicts with this recovery")
        }
        match operation.status.as_str() {
            "RESTORING" => Ok(RestoreState::Running),
            "RESTORED" => Ok(RestoreState::Restored),
            "RESTORE_FAILED" => bail!("ClickHouse restore failed: {}", operation.error),
            status => bail!("ClickHouse returned an unknown restore status: {status}"),
        }
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

        validate_engines(self, pipelines, &engines, require_state).await
    }

    async fn default_dedup(&self, engine: &Engine) -> Result<u64> {
        let (table, setting) = engine.dedup_setting();
        let defaults: Vec<SettingValue> = self
            .json_each_row(&format!(
                "SELECT value FROM {table} WHERE name = '{setting}' FORMAT JSONEachRow"
            ))
            .await?;
        let [default] = defaults.as_slice() else {
            bail!("ClickHouse did not return exactly one value for {setting}")
        };
        default
            .value
            .parse()
            .with_context(|| format!("ClickHouse returned an invalid {setting}"))
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

async fn validate_engines(
    clickhouse: &ClickHouse,
    pipelines: &[Pipeline],
    engines: &[TableEngine],
    require_state: bool,
) -> Result<()> {
    for pipeline in pipelines {
        let engine = pipeline_engine(pipeline, engines, require_state)?;
        engine.validate(clickhouse).await?;
    }
    Ok(())
}

fn pipeline_engine<'a>(
    pipeline: &Pipeline,
    engines: &'a [TableEngine],
    require_state: bool,
) -> Result<&'a TableEngine> {
    let database = &pipeline.database;
    let table = &pipeline.table;
    let engine = engines
        .iter()
        .find(|engine| engine.database == *database && engine.name == *table)
        .with_context(|| format!("backup table does not exist: {database}.{table}"))?;
    if require_state {
        let state_table = &pipeline.state_table;
        let state = engines
            .iter()
            .find(|engine| engine.database == *database && engine.name == *state_table)
            .with_context(|| format!("backup table does not exist: {database}.{state_table}"))?;
        if state.engine != Engine::KeeperMap {
            bail!(
                "backup manifests require {database}.{state_table} to use KeeperMap, found {}",
                state.engine
            )
        }
    }
    Ok(engine)
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
            engine: value.to_owned().into(),
            create_table_query: format!("CREATE TABLE history.{name} ENGINE = {value}"),
        }
    }

    #[test]
    fn accepts_merge_tree_family_and_keeper_map() {
        let engines = [
            engine("records", "ReplicatedMergeTree"),
            engine("records_state", "KeeperMap"),
        ];
        let target = pipeline_engine(&pipeline(), &engines, true).unwrap();
        assert!(target.validate_with_default(1000).is_ok());
    }

    #[test]
    fn rejects_non_merge_tree_missing_deduplication_and_missing_state() {
        assert!(
            engine("records", "Log")
                .validate_with_default(1000)
                .is_err()
        );
        assert!(
            engine("records", "MergeTree")
                .validate_with_default(0)
                .is_err()
        );
        let engines = [engine("records", "MergeTree")];
        let error = pipeline_engine(&pipeline(), &engines, true).unwrap_err();
        assert!(error.to_string().contains("history.records_state"));

        let engines = [engine("records_state", "KeeperMap")];
        let error = pipeline_engine(&pipeline(), &engines, true).unwrap_err();
        assert!(error.to_string().contains("history.records"));
    }

    #[test]
    fn full_backups_also_require_safe_target_and_keeper_map() {
        let engines = [
            engine("records", "MergeTree"),
            engine("records_state", "Log"),
        ];
        let error = pipeline_engine(&pipeline(), &engines, true).unwrap_err();
        assert!(error.to_string().contains("to use KeeperMap"));
    }

    #[test]
    fn rejects_disabled_replicated_default() {
        assert!(
            engine("records", "ReplicatedMergeTree")
                .validate_with_default(0)
                .is_err()
        );
    }

    #[test]
    fn target_preflight_does_not_require_connector_state_yet() {
        let engines = [engine("records", "ReplicatedMergeTree")];
        let target = pipeline_engine(&pipeline(), &engines, false).unwrap();
        assert!(target.validate_with_default(1000).is_ok());
    }

    #[test]
    fn table_settings_override_inherited_deduplication_defaults() {
        for (name, setting) in [
            ("MergeTree", "non_replicated_deduplication_window"),
            ("ReplicatedMergeTree", "replicated_deduplication_window"),
            ("SharedMergeTree", "replicated_deduplication_window"),
            (
                "SharedReplacingMergeTree",
                "replicated_deduplication_window",
            ),
        ] {
            for (explicit, default, valid) in [
                (None, 0, false),
                (None, 1000, true),
                (Some(0), 1000, false),
                (Some(5), 0, true),
            ] {
                let mut target = engine("records", name);
                if let Some(window) = explicit {
                    target
                        .create_table_query
                        .push_str(&format!(" SETTINGS {setting} = {window}"));
                }
                assert_eq!(
                    target.validate_with_default(default).is_ok(),
                    valid,
                    "{name}: explicit={explicit:?}, default={default}"
                );
            }
        }
    }

    #[test]
    fn reads_explicit_deduplication_settings() {
        for setting in [
            "replicated_deduplication_window",
            "non_replicated_deduplication_window",
        ] {
            let query = "CREATE TABLE history.records ENGINE = MergeTree";
            assert_eq!(table_setting(query, setting), None);
            for window in [0, 5, 123] {
                let query =
                    format!("{query} SETTINGS {setting} = {window}, index_granularity = 8192");
                assert_eq!(table_setting(&query, setting), Some(window));
            }
        }
    }

    #[test]
    fn deserializes_and_classifies_supported_engines() {
        for base in [
            "MergeTree",
            "ReplacingMergeTree",
            "SummingMergeTree",
            "AggregatingMergeTree",
            "CollapsingMergeTree",
            "VersionedCollapsingMergeTree",
            "GraphiteMergeTree",
            "CoalescingMergeTree",
        ] {
            for prefix in ["", "Replicated", "Shared"] {
                let name = format!("{prefix}{base}");
                let engine: Engine = serde_json::from_value(serde_json::json!(name)).unwrap();
                assert!(engine.is_supported(), "{name}");
                assert_eq!(engine.is_replicated(), !prefix.is_empty(), "{name}");
                assert_eq!(engine.to_string(), name);
                let expected = if prefix.is_empty() {
                    (
                        "system.merge_tree_settings",
                        "non_replicated_deduplication_window",
                    )
                } else {
                    (
                        "system.replicated_merge_tree_settings",
                        "replicated_deduplication_window",
                    )
                };
                assert_eq!(engine.dedup_setting(), expected, "{name}");
            }
        }
        assert_eq!(
            serde_json::from_str::<Engine>("\"KeeperMap\"").unwrap(),
            Engine::KeeperMap
        );
        assert!(!Engine::KeeperMap.is_supported());
        for name in ["Log", "UnknownMergeTree", "UnknownReplicatedMergeTree"] {
            let engine: Engine = serde_json::from_value(serde_json::json!(name)).unwrap();
            assert_eq!(engine, Engine::Other(name.to_owned()));
            assert!(!engine.is_supported());
        }
    }
}
