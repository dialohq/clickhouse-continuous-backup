use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;
use zookeeper_client::{Acls, Client, CreateMode, Error as KeeperError, SessionState};

/// One token's pause: the connectors it holds and when it lapses unless renewed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Pause {
    pub(super) connectors: Vec<String>,
    pub(super) expires_at: DateTime<Utc>,
}

/// The pauses in Keeper when they were read, by token.
///
/// A connector can be held by several tokens and is only resumed once the last one releases it.
/// Connectors paused outside this server are never held, so they are never resumed here.
pub(super) struct Snapshot(HashMap<Uuid, Pause>);

impl Snapshot {
    /// Whether any token holds the connector, expired ones included until the sweep removes them.
    pub(super) fn is_held(&self, connector: &str) -> bool {
        self.0
            .values()
            .any(|pause| pause.connectors.iter().any(|held| held == connector))
    }

    pub(super) fn get(&self, token: &Uuid) -> Option<&Pause> {
        self.0.get(token)
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    /// Tokens whose pause lapsed before `now`.
    pub(super) fn expired(&self, now: DateTime<Utc>) -> Vec<Uuid> {
        self.0
            .iter()
            .filter(|(_, pause)| pause.expires_at <= now)
            .map(|(token, _)| *token)
            .collect()
    }

    /// The token's connectors that no other live token holds, i.e. the ones releasing it resumes.
    ///
    /// Expired tokens don't count: their connectors are due to be resumed anyway.
    pub(super) fn held_only_by(&self, token: &Uuid, now: DateTime<Utc>) -> Vec<String> {
        let Some(pause) = self.get(token) else {
            return Vec::new();
        };
        pause
            .connectors
            .iter()
            .filter(|connector| {
                !self.0.iter().any(|(other, pause)| {
                    other != token && pause.expires_at > now && pause.connectors.contains(connector)
                })
            })
            .cloned()
            .collect()
    }
}

/// The pause server's nodes in Keeper, under its configured path:
///
/// ```text
/// <path>/tokens/<token>   one per token, the JSON of its `Pause`
/// <path>/leader           the lock: ephemeral, so it goes with the session of the server holding it
/// ```
#[derive(Clone)]
pub(super) struct TokenStore {
    client: Client,
    path: String,
}

impl TokenStore {
    pub(super) async fn connect(address: &str, path: &str) -> Result<Self> {
        // ClickHouse Keeper rejects `Create2`, which the client sends unless the server is 3.4.
        let client = Client::connector()
            .with_server_version(3, 4, 0)
            .connect(address)
            .await
            .with_context(|| format!("failed to connect to Keeper at {address}"))?;
        let store = Self {
            client,
            path: path.to_owned(),
        };
        store
            .client
            .mkdir(
                &store.tokens_path(),
                &CreateMode::Persistent.with_acls(Acls::anyone_all()),
            )
            .await
            .with_context(|| format!("failed to create {} in Keeper", store.tokens_path()))?;
        Ok(store)
    }

    /// Creates the lock node, failing if another server already holds it.
    ///
    /// Keeper removes it when this server's session ends, whether it stops or dies, so the next
    /// server can start once the session has timed out.
    pub(super) async fn lock(&self) -> Result<()> {
        let path = self.lock_path();
        match self
            .client
            .create(
                &path,
                &[],
                &CreateMode::Ephemeral.with_acls(Acls::anyone_all()),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(KeeperError::NodeExists) => {
                bail!("{path} exists in Keeper: another pause server holds the lock")
            }
            Err(error) => Err(error).with_context(|| format!("failed to create {path} in Keeper")),
        }
    }

    /// Resolves once the Keeper session is gone for good, and with it the lock.
    pub(super) async fn session_ended(&self) -> SessionState {
        let mut watcher = self.client.state_watcher();
        loop {
            let state = watcher.state();
            if state.is_terminated() {
                return state;
            }
            watcher.changed().await;
        }
    }

    /// Reads every token written by `save`.
    pub(super) async fn read(&self) -> Result<Snapshot> {
        let tokens_path = self.tokens_path();
        let names = self
            .client
            .list_children(&tokens_path)
            .await
            .with_context(|| format!("failed to list {tokens_path} in Keeper"))?;
        let mut tokens = HashMap::new();
        for name in names {
            let path = format!("{tokens_path}/{name}");
            let token = name
                .parse()
                .with_context(|| format!("{path} in Keeper is not a token"))?;
            let (data, _) = self
                .client
                .get_data(&path)
                .await
                .with_context(|| format!("failed to read {path} from Keeper"))?;
            let pause = serde_json::from_slice(&data)
                .with_context(|| format!("invalid pause in {path} in Keeper"))?;
            tokens.insert(token, pause);
        }
        Ok(Snapshot(tokens))
    }

    /// Creates or replaces the token's node.
    pub(super) async fn save(&self, token: &Uuid, pause: &Pause) -> Result<()> {
        let path = self.token_path(token);
        let data = serde_json::to_vec(pause)?;
        let result = match self.client.set_data(&path, &data, None).await {
            Err(KeeperError::NoNode) => self
                .client
                .create(
                    &path,
                    &data,
                    &CreateMode::Persistent.with_acls(Acls::anyone_all()),
                )
                .await
                .map(|_| ()),
            result => result.map(|_| ()),
        };
        result.with_context(|| format!("failed to save {path} to Keeper"))?;
        debug!(%token, "saved pause");
        Ok(())
    }

    /// Deletes the token's node; a node that is already gone is fine.
    pub(super) async fn remove(&self, token: &Uuid) -> Result<()> {
        let path = self.token_path(token);
        match self.client.delete(&path, None).await {
            Ok(()) | Err(KeeperError::NoNode) => {
                debug!(%token, "removed pause");
                Ok(())
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to remove {path} from Keeper"))
            }
        }
    }

    pub(super) fn lock_path(&self) -> String {
        format!("{}/leader", self.path)
    }

    fn tokens_path(&self) -> String {
        format!("{}/tokens", self.path)
    }

    fn token_path(&self, token: &Uuid) -> String {
        format!("{}/{token}", self.tokens_path())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use pretty_assertions::assert_eq;

    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn pause(connectors: &[&str], expires_in: i64) -> Pause {
        Pause {
            connectors: connectors
                .iter()
                .map(|&connector| connector.to_owned())
                .collect(),
            expires_at: now() + TimeDelta::seconds(expires_in),
        }
    }

    fn snapshot(pauses: impl IntoIterator<Item = (u128, Pause)>) -> Snapshot {
        Snapshot(
            pauses
                .into_iter()
                .map(|(token, pause)| (Uuid::from_u128(token), pause))
                .collect(),
        )
    }

    #[test]
    fn connector_is_released_only_by_its_last_live_holder() {
        let snapshot = snapshot([(1, pause(&["a", "b"], 10)), (2, pause(&["b"], 10))]);
        assert_eq!(
            snapshot.held_only_by(&Uuid::from_u128(1), now()),
            vec!["a".to_owned()]
        );
        assert!(snapshot.held_only_by(&Uuid::from_u128(2), now()).is_empty());
        assert!(snapshot.held_only_by(&Uuid::from_u128(3), now()).is_empty());
    }

    #[test]
    fn expired_holders_do_not_keep_a_connector_paused() {
        let snapshot = snapshot([(1, pause(&["a"], 0)), (2, pause(&["a"], -5))]);
        for token in [1, 2] {
            assert_eq!(
                snapshot.held_only_by(&Uuid::from_u128(token), now()),
                vec!["a".to_owned()]
            );
        }
        assert!(snapshot.is_held("a"));
        assert!(!snapshot.is_held("b"));
    }

    #[test]
    fn only_lapsed_pauses_are_expired() {
        let snapshot = snapshot([(1, pause(&["a"], 0)), (2, pause(&["a"], 1))]);
        assert_eq!(snapshot.expired(now()), vec![Uuid::from_u128(1)]);
    }
}
