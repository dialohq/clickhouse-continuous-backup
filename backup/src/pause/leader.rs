use std::{env, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    sync::Mutex,
    time::{MissedTickBehavior, interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zookeeper_client::{Acls, Client, LockOptions, LockPrefix, OwnedLockClient, SessionState};

use super::{
    AppState, Term,
    handlers::{sweep, sweep_periodically},
    labels::PodLabels,
    registry::{TokenStore, persistent},
};
use crate::config::PauseServerConfig;

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const LABEL_INTERVAL: Duration = Duration::from_secs(10);

pub(super) async fn lead(
    state: &AppState,
    config: &PauseServerConfig,
    labels: Option<&PodLabels>,
    shutdown: &CancellationToken,
) {
    if let Some(labels) = labels
        && let Err(error) = labels.release().await
    {
        warn!("failed to take the active label off this pod: {error:#}");
    }
    while !shutdown.is_cancelled() {
        let client = match connect(&config.keeper_address).await {
            Ok(client) => client,
            Err(error) => {
                error!("{error:#}");
                tokio::select! {
                    () = sleep(RECONNECT_DELAY) => continue,
                    () = shutdown.cancelled() => return,
                }
            }
        };
        let Some(lock) = contend(&client, &config.keeper_path, labels, shutdown).await else {
            continue;
        };
        let lock = Arc::new(lock);
        info!(lock = lock.lock_path(), "acquired the leader lock, leading");
        match TokenStore::new(lock.clone(), &config.keeper_path).await {
            Ok(store) => lead_while_connected(state, &client, store, labels, shutdown).await,
            Err(error) => error!("cannot lead: {error:#}"),
        }
        if let Some(labels) = labels
            && let Err(error) = labels.release().await
        {
            warn!("failed to take the active label off this pod: {error:#}");
        }
        if shutdown.is_cancelled()
            && let Err(error) = client.delete(lock.lock_path(), None).await
        {
            warn!("failed to delete the leader lock on shutdown: {error:#}");
        }
        info!("stopped leading");
    }
}

async fn connect(address: &str) -> Result<Client> {
    Client::connector()
        .with_server_version(3, 4, 0)
        .connect(address)
        .await
        .with_context(|| format!("failed to connect to Keeper at {address}"))
}

async fn contend(
    client: &Client,
    path: &str,
    labels: Option<&PodLabels>,
    shutdown: &CancellationToken,
) -> Option<OwnedLockClient> {
    let directory = format!("{path}/leader");
    let prefix = match LockPrefix::new_curator(&directory, "latch-") {
        Ok(prefix) => prefix,
        Err(error) => {
            error!(directory, "invalid leader lock path: {error}");
            shutdown.cancelled().await;
            return None;
        }
    };
    let options = LockOptions::new(Acls::anyone_all())
        .with_ancestor_options(persistent())
        .expect("persistent nodes are valid ancestors");
    let identity = labels
        .map(|labels| labels.pod().to_owned())
        .or_else(|| env::var("HOSTNAME").ok())
        .unwrap_or_default();
    info!(directory, "standing by for the leader lock");
    tokio::select! {
        lock = client.lock(prefix, identity.as_bytes(), options) => match lock {
            Ok(lock) => Some(lock.into_owned()),
            Err(error) => {
                warn!("lost the Keeper session while standing by: {error}");
                None
            }
        },
        state = session_ended(client) => {
            warn!(%state, "lost the Keeper session while standing by");
            None
        }
        () = shutdown.cancelled() => None,
    }
}

async fn lead_while_connected(
    state: &AppState,
    client: &Client,
    store: TokenStore,
    labels: Option<&PodLabels>,
    shutdown: &CancellationToken,
) {
    let mut session = client.state_watcher();
    loop {
        loop {
            match session.state() {
                SessionState::SyncConnected => break,
                state if state.is_terminated() => {
                    warn!(%state, "lost the Keeper session and with it the leader lock");
                    return;
                }
                _ => {}
            }
            tokio::select! {
                _ = session.changed() => {}
                () = shutdown.cancelled() => return,
            }
        }

        let term = Arc::new(Term {
            store: Mutex::new(store.clone()),
            ended: CancellationToken::new(),
        });
        match sweep(state, &*term.store.lock().await).await {
            Ok(tokens) => info!(tokens, "loaded pauses"),
            Err(error) => error!("failed to sweep pauses: {error:#}"),
        }
        let sweeping = tokio::spawn(sweep_periodically(state.clone(), term.clone()));
        let labelling = labels.map(|labels| tokio::spawn(keep_labelled(labels.clone())));
        state.set_term(Some(term.clone()));
        info!("serving as the leader");

        let ended = tokio::select! {
            state = until_not_connected(&mut session) => Some(state),
            () = shutdown.cancelled() => None,
        };

        state.set_term(None);
        term.ended.cancel();
        sweeping.abort();
        if let Some(labelling) = labelling {
            labelling.abort();
        }

        match ended {
            None => return,
            Some(state) if state.is_terminated() => {
                warn!(%state, "lost the Keeper session and with it the leader lock");
                return;
            }
            Some(state) => {
                warn!(%state, "lost the Keeper connection, refusing requests until it's back")
            }
        }
    }
}

async fn until_not_connected(session: &mut zookeeper_client::StateWatcher) -> SessionState {
    loop {
        let state = session.changed().await;
        if state != SessionState::SyncConnected {
            return state;
        }
    }
}

async fn session_ended(client: &Client) -> SessionState {
    let mut session = client.state_watcher();
    loop {
        let state = session.state();
        if state.is_terminated() {
            return state;
        }
        session.changed().await;
    }
}

async fn keep_labelled(labels: PodLabels) {
    let mut ticks = interval(LABEL_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if let Err(error) = labels.claim().await {
            error!("failed to label this pod as the leader: {error:#}");
        }
    }
}
