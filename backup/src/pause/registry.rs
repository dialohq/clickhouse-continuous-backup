use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs::{self, File, OpenOptions, TryLockError},
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use uuid::Uuid;

/// Pauses handed out by `/pause`, kept in memory until `/resume` releases them.
///
/// A connector can be held by several tokens and is only resumed once the last one releases it.
/// Connectors paused outside this server are never held, so they are never resumed here.
#[derive(Default)]
pub(super) struct Pauses {
    tokens: HashMap<Uuid, Vec<String>>,
    holders: HashMap<String, HashSet<Uuid>>,
}

impl Pauses {
    /// Reads the tokens written by `save`; a missing file means there are none.
    pub(super) fn load(path: &Path) -> Result<Self> {
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read pauses from {}", path.display()));
            }
        };
        let tokens: HashMap<Uuid, Vec<String>> = serde_json::from_str(&contents)
            .with_context(|| format!("invalid pauses in {}", path.display()))?;
        let mut pauses = Self::default();
        for (token, connectors) in tokens {
            pauses.hold(token, connectors);
        }
        Ok(pauses)
    }

    /// Replaces the file atomically, so a crash leaves either the previous or the new tokens.
    pub(super) fn save(&self, path: &Path) -> Result<()> {
        let temporary = with_suffix(path, ".tmp");
        let write = || -> std::io::Result<()> {
            let mut file = File::create(&temporary)?;
            serde_json::to_writer(&mut file, &self.tokens)?;
            file.sync_all()?;
            fs::rename(&temporary, path)?;
            // Persist the rename itself.
            let directory = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty());
            File::open(directory.unwrap_or(Path::new(".")))?.sync_all()
        };
        write().with_context(|| format!("failed to save pauses to {}", path.display()))
    }

    pub(super) fn is_held(&self, connector: &str) -> bool {
        self.holders.contains_key(connector)
    }

    pub(super) fn connectors(&self, token: &Uuid) -> Option<&[String]> {
        self.tokens.get(token).map(Vec::as_slice)
    }

    pub(super) fn hold(&mut self, token: Uuid, connectors: Vec<String>) {
        for connector in &connectors {
            self.holders
                .entry(connector.clone())
                .or_default()
                .insert(token);
        }
        self.tokens.insert(token, connectors);
    }

    /// The token's connectors that no other token holds, i.e. the ones releasing it would resume.
    pub(super) fn held_only_by(&self, token: &Uuid) -> Vec<String> {
        self.connectors(token)
            .unwrap_or_default()
            .iter()
            .filter(|connector| {
                self.holders
                    .get(*connector)
                    .is_none_or(|holders| holders.len() == 1)
            })
            .cloned()
            .collect()
    }

    /// Removes the token and returns its connectors that no other token holds any more.
    pub(super) fn release(&mut self, token: &Uuid) -> Option<Vec<String>> {
        let connectors = self.tokens.remove(token)?;
        let released = connectors
            .into_iter()
            .filter(|connector| match self.holders.get_mut(connector) {
                Some(holders) => {
                    holders.remove(token);
                    if holders.is_empty() {
                        self.holders.remove(connector);
                        true
                    } else {
                        false
                    }
                }
                None => true,
            })
            .collect();
        Some(released)
    }
}

/// Locks `<file>.lock` rather than the file itself, which `Pauses::save` replaces on every write.
pub(super) fn lock_pauses_file(path: &Path) -> Result<File> {
    let lock_path = with_suffix(path, ".lock");
    let lock = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("failed to open {}", lock_path.display()))?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(TryLockError::WouldBlock) => bail!(
            "{} is locked: another pause server is using {}",
            lock_path.display(),
            path.display()
        ),
        Err(TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("failed to lock {}", lock_path.display()))
        }
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(path);
    path.push(suffix);
    path.into()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn connector_is_released_only_by_its_last_holder() {
        let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut pauses = Pauses::default();
        pauses.hold(first, vec!["a".to_owned(), "b".to_owned()]);
        pauses.hold(second, vec!["b".to_owned()]);

        assert_eq!(pauses.held_only_by(&first), vec!["a".to_owned()]);
        assert_eq!(pauses.release(&first), Some(vec!["a".to_owned()]));
        assert_eq!(pauses.held_only_by(&second), vec!["b".to_owned()]);
        assert!(!pauses.is_held("a"));
        assert!(pauses.is_held("b"));
        assert_eq!(pauses.release(&second), Some(vec!["b".to_owned()]));
        assert!(!pauses.is_held("b"));
    }

    #[test]
    fn unknown_or_released_token_is_not_found() {
        let token = Uuid::from_u128(1);
        let mut pauses = Pauses::default();
        assert_eq!(pauses.release(&token), None);
        pauses.hold(token, vec!["a".to_owned()]);
        assert!(pauses.release(&token).is_some());
        assert_eq!(pauses.release(&token), None);
        assert_eq!(pauses.connectors(&token), None);
    }

    #[test]
    fn saved_pauses_load_back_with_holders() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pauses.json");
        assert!(Pauses::load(&path).unwrap().tokens.is_empty());

        let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut pauses = Pauses::default();
        pauses.hold(first, vec!["a".to_owned(), "b".to_owned()]);
        pauses.hold(second, vec!["b".to_owned()]);
        pauses.save(&path).unwrap();

        let loaded = Pauses::load(&path).unwrap();
        assert_eq!(loaded.tokens, pauses.tokens);
        assert_eq!(loaded.holders, pauses.holders);
        assert!(!with_suffix(&path, ".tmp").exists());
    }

    #[test]
    fn second_lock_on_the_same_file_fails() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pauses.json");
        let lock = lock_pauses_file(&path).unwrap();
        assert!(lock_pauses_file(&path).is_err());
        drop(lock);
        assert!(lock_pauses_file(&path).is_ok());
    }
}
