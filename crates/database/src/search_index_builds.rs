//! Registry of text/vector index builds that are between "started uploading
//! segments" and "metadata committed (or build abandoned)".
//!
//! A search index build (a flush or a compaction) uploads new segment
//! objects to search storage first and commits the `_index` revision that
//! references them afterwards. Between those two points the objects exist in
//! storage without any retained metadata mentioning them, which is exactly
//! what a garbage collector of search storage would take for garbage. The
//! registry lets the collector see that such a build is in progress: a round
//! that started while a build was already running cannot prove that the
//! build's objects are unreferenced, and must not delete anything.
//!
//! Builds register through [`ActiveSearchIndexBuilds::begin`], which returns
//! a guard; dropping the guard — after the commit returned, or on any error
//! path out of the build — ends the registration. The registry records only
//! start times, never object keys, so it stays correct no matter how a build
//! names or stages its objects.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{
            AtomicU64,
            Ordering,
        },
        Arc,
    },
    time::SystemTime,
};

use parking_lot::Mutex;

#[derive(Debug, Default)]
pub struct ActiveSearchIndexBuilds {
    next_id: AtomicU64,
    started: Mutex<BTreeMap<u64, SystemTime>>,
}

impl ActiveSearchIndexBuilds {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a build that starts now. The registration ends when the
    /// returned guard is dropped.
    pub fn begin(self: &Arc<Self>, started: SystemTime) -> ActiveSearchIndexBuildGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.started.lock().insert(id, started);
        ActiveSearchIndexBuildGuard {
            registry: self.clone(),
            id,
        }
    }

    /// Start time of the longest-running registered build, if any.
    pub fn oldest_started(&self) -> Option<SystemTime> {
        self.started.lock().values().min().copied()
    }

    pub fn len(&self) -> usize {
        self.started.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[must_use = "dropping the guard ends the registration"]
pub struct ActiveSearchIndexBuildGuard {
    registry: Arc<ActiveSearchIndexBuilds>,
    id: u64,
}

impl Drop for ActiveSearchIndexBuildGuard {
    fn drop(&mut self) {
        self.registry.started.lock().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{
        Duration,
        SystemTime,
    };

    use super::ActiveSearchIndexBuilds;

    #[test]
    fn registrations_end_when_their_guard_drops() {
        let registry = ActiveSearchIndexBuilds::new();
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        assert!(registry.is_empty());
        assert_eq!(registry.oldest_started(), None);

        let first = registry.begin(t0);
        let second = registry.begin(t0 + Duration::from_secs(5));
        assert_eq!(registry.len(), 2);
        assert_eq!(registry.oldest_started(), Some(t0));

        drop(first);
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.oldest_started(), Some(t0 + Duration::from_secs(5)));

        drop(second);
        assert!(registry.is_empty());
        assert_eq!(registry.oldest_started(), None);
    }

    #[test]
    fn guard_drops_on_the_error_path_too() {
        let registry = ActiveSearchIndexBuilds::new();
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let build = || -> anyhow::Result<()> {
            let _guard = registry.begin(t0);
            anyhow::bail!("upload failed");
        };
        assert!(build().is_err());
        assert!(registry.is_empty());
    }
}
