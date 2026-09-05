//! Garbage collection of unreferenced text/vector index segments in search
//! storage.
//!
//! Every text and vector index segment lives in the deployment's search
//! storage (`StorageUseCase::SearchIndexes`; text and vector index workers
//! share that one `Storage`, and nothing else writes to it) and is referenced
//! by object key from the index's metadata document in the `_index` system
//! table. Flushes and compactions upload new segments and then commit a new
//! `_index` revision that points at them; the segments they replace are never
//! deleted. Without external lifecycle rules (which self-hosted deployments do
//! not have) search storage therefore only ever grows.
//!
//! This collector reverses that from the metadata side:
//!
//! 1. Build a keep set of every string that appears in any `_index` revision
//!    the database still retains: every document in the current `_index`
//!    snapshot, plus every revision (and the revision it replaced) written
//!    since the document retention floor. The harvest is deliberately
//!    shape-agnostic: it does not know which fields hold object keys, so a new
//!    or renamed metadata field can never silently unprotect a segment. Reading
//!    the replaced revision of each change is what makes the set complete for
//!    MVCC readers: a segment that was swapped out a moment ago is still
//!    visible to a transaction at an older snapshot, and shows up here as the
//!    `prev_rev` of the change that replaced it. Every document of the current
//!    snapshot must additionally parse as `TabletIndexMetadata`, so a metadata
//!    format this code does not understand stops the round instead of being
//!    harvested incompletely.
//! 2. List search storage and select objects that are absent from the keep set
//!    and older than `SEARCH_SEGMENT_GC_MIN_OBJECT_AGE`. The age floor covers
//!    the window between an upload finishing and its `_index` revision
//!    committing (a segment is uploaded first and referenced afterwards); the
//!    wall clock is read before the keep set is built, so anything uploaded
//!    during the scan is too young by construction. The knob cannot go below
//!    `MIN_OBJECT_AGE_FLOOR`.
//! 3. Refuse to delete anything when the keep set protects none of the listed
//!    objects: a deployment whose search storage does not match its metadata
//!    (wrong directory, wrong prefix) would otherwise lose every segment it
//!    has. Dry runs still report in that case, flagged as refused.
//!
//! Any error while building the keep set aborts the round without deleting; a
//! failed delete is counted and the round continues. Deletes are capped per
//! round by `SEARCH_SEGMENT_GC_MAX_DELETES_PER_ROUND` so one round cannot hold
//! the cleanup worker for long after a large backlog. `SEARCH_SEGMENT_GC_MODE`
//! selects between doing nothing, only reporting, and deleting.
//!
//! Operational limits, deliberately not solved here: the collector assumes
//! this backend is the only writer of its search storage. Two backends sharing
//! one storage directory or S3 prefix (a clone restored without a fresh
//! prefix) would each treat the other's segments as orphans; leave the knob
//! `off` on any such deployment. Crash-abandoned S3 multipart uploads are
//! invisible to `ListObjectsV2` and are left to a bucket lifecycle rule.

use std::{
    collections::BTreeSet,
    fmt,
    str::FromStr,
    sync::Arc,
    time::{
        Duration,
        SystemTime,
    },
};

use anyhow::Context as _;
use common::{
    bootstrap_model::index::TabletIndexMetadata,
    document::{
        ParseDocument,
        ParsedDocument,
    },
    knobs::{
        SEARCH_SEGMENT_GC_MAX_DELETES_PER_ROUND,
        SEARCH_SEGMENT_GC_MIN_OBJECT_AGE,
        SEARCH_SEGMENT_GC_MODE,
    },
    persistence::TimestampRange,
    query::Order,
    runtime::{
        RateLimiter,
        Runtime,
    },
    types::RepeatableTimestamp,
};
use database::Database;
use futures::{
    pin_mut,
    TryStreamExt,
};
use storage::{
    ObjectListing,
    Storage,
};
use value::{
    sha256::Sha256,
    ConvexObject,
    ConvexValue,
};

use super::metrics::{
    log_search_segment_gc_objects,
    log_search_segment_gc_round,
    search_segment_gc_timer,
};

/// How many objects are listed per page by `_index` scans of the keep set.
const KEEP_SET_PAGE_SIZE: usize = 100;

/// Lowest accepted `SEARCH_SEGMENT_GC_MIN_OBJECT_AGE`. Age is what protects a
/// segment between its upload finishing and its `_index` revision committing,
/// so the knob must never be able to shrink that protection to nothing.
pub const MIN_OBJECT_AGE_FLOOR: Duration = Duration::from_secs(10 * 60);

/// How many orphan keys a round logs individually; the rest are covered by
/// the count and digest in the summary line.
const LOGGED_KEY_SAMPLE: usize = 50;

/// What the collector is allowed to do, from `SEARCH_SEGMENT_GC_MODE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchSegmentGcMode {
    /// Do nothing (the default).
    Off,
    /// Build the keep set, list storage, and log what would be deleted.
    DryRun,
    /// Delete orphaned objects.
    Delete,
}

impl FromStr for SearchSegmentGcMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "off" => Ok(Self::Off),
            "dry_run" => Ok(Self::DryRun),
            "delete" => Ok(Self::Delete),
            other => anyhow::bail!(
                "Invalid SEARCH_SEGMENT_GC_MODE {other:?}: expected one of off, dry_run, delete"
            ),
        }
    }
}

impl fmt::Display for SearchSegmentGcMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Off => "off",
            Self::DryRun => "dry_run",
            Self::Delete => "delete",
        };
        write!(f, "{s}")
    }
}

/// The collector's configuration, validated from the knobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchSegmentGcConfig {
    pub mode: SearchSegmentGcMode,
    pub min_object_age: Duration,
    pub max_deletes_per_round: usize,
}

impl SearchSegmentGcConfig {
    /// Validate a configuration. An unparseable mode or an age below the
    /// floor is an error rather than a fallback, so a typo or an unsafe value
    /// shows up in the logs every round instead of silently meaning "off" or
    /// "delete".
    pub fn new(
        mode: &str,
        min_object_age: Duration,
        max_deletes_per_round: usize,
    ) -> anyhow::Result<Self> {
        let mode = mode.parse()?;
        anyhow::ensure!(
            min_object_age >= MIN_OBJECT_AGE_FLOOR,
            "SEARCH_SEGMENT_GC_MIN_OBJECT_AGE_SECONDS={} is below the floor of {}s",
            min_object_age.as_secs(),
            MIN_OBJECT_AGE_FLOOR.as_secs(),
        );
        Ok(Self {
            mode,
            min_object_age,
            max_deletes_per_round,
        })
    }

    /// The configuration from the `SEARCH_SEGMENT_GC_*` knobs.
    pub fn from_knobs() -> anyhow::Result<Self> {
        Self::new(
            &SEARCH_SEGMENT_GC_MODE,
            *SEARCH_SEGMENT_GC_MIN_OBJECT_AGE,
            *SEARCH_SEGMENT_GC_MAX_DELETES_PER_ROUND,
        )
    }
}

/// Add every string value reachable from `value` to `out`.
fn collect_strings(value: &ConvexValue, out: &mut BTreeSet<String>) {
    match value {
        ConvexValue::Null
        | ConvexValue::Int64(_)
        | ConvexValue::Float64(_)
        | ConvexValue::Boolean(_)
        | ConvexValue::Bytes(_) => {},
        ConvexValue::String(s) => {
            out.insert((**s).to_owned());
        },
        ConvexValue::Array(items) => {
            for item in items.iter() {
                collect_strings(item, out);
            }
        },
        ConvexValue::Object(object) => collect_object_strings(object, out),
    }
}

/// Add every string value reachable from `object` to `out`.
fn collect_object_strings(object: &ConvexObject, out: &mut BTreeSet<String>) {
    for (_, value) in object.iter() {
        collect_strings(value, out);
    }
}

/// Every string referenced by an `_index` revision the database still retains,
/// with the bookkeeping needed to explain the round in the log.
pub struct KeepSet {
    pub strings: BTreeSet<String>,
    /// Documents in the `_index` snapshot at `latest_ts`.
    pub snapshot_documents: usize,
    /// Revisions (and replaced revisions) read from the documents log.
    pub log_revisions: usize,
    /// Start of the documents-log range that was read.
    pub floor: RepeatableTimestamp,
    /// Snapshot timestamp the keep set is complete for.
    pub latest_ts: RepeatableTimestamp,
}

/// Build the keep set: strings from every `_index` document in the latest
/// snapshot, plus every revision and the revision it replaced in the documents
/// log since the document retention floor.
///
/// Any failure to read or interpret a revision is an error; the caller must
/// then delete nothing, because the keep set could be incomplete.
async fn search_segment_keep_set<RT: Runtime>(
    database: &Database<RT>,
    rate_limiter: &RateLimiter<RT>,
) -> anyhow::Result<KeepSet> {
    let floor = database
        .retention_validator()
        .min_document_snapshot_ts()
        .await?;
    let (latest_ts, snapshot) = database.latest_ts_and_snapshot()?;
    anyhow::ensure!(
        *floor <= *latest_ts,
        "Document retention floor {} is ahead of the latest snapshot {}",
        *floor,
        *latest_ts,
    );
    let index_tablet_id = snapshot.index_registry.index_table();
    let index_by_id = snapshot
        .index_registry
        .must_get_by_id(index_tablet_id)?
        .id();

    let mut strings = BTreeSet::new();

    let mut snapshot_documents = 0;
    let snapshot_stream = database
        .table_iterator(latest_ts, KEEP_SET_PAGE_SIZE)
        .stream_documents_in_table(index_tablet_id, index_by_id, None);
    pin_mut!(snapshot_stream);
    while let Some(document) = snapshot_stream.try_next().await? {
        // The strings are the root set; the typed parse only proves that the
        // current metadata format is one this code was written against.
        collect_object_strings(&document.value.value().0, &mut strings);
        let _: ParsedDocument<TabletIndexMetadata> = document
            .value
            .parse()
            .context("Cannot parse an _index document as index metadata")?;
        snapshot_documents += 1;
    }

    let mut log_revisions = 0;
    let revision_stream = database.load_revision_pairs_in_table(
        index_tablet_id,
        TimestampRange::new(*floor..=*latest_ts),
        Order::Asc,
        rate_limiter,
    );
    pin_mut!(revision_stream);
    while let Some(pair) = revision_stream.try_next().await? {
        if let Some(document) = &pair.rev.document {
            collect_object_strings(&document.value().0, &mut strings);
        }
        log_revisions += 1;
        if let Some(prev_rev) = &pair.prev_rev {
            // `prev_rev` without a document means the replaced revision
            // exists but its value has already been garbage collected by
            // document retention. That cannot normally happen inside the
            // range we read (retention only removes the predecessor of a
            // revision that is itself older than the floor), so treat it
            // as "we cannot prove what this revision referenced".
            let document = prev_rev.document.as_ref().with_context(|| {
                format!(
                    "_index revision {}@{} has a replaced revision at {} whose value is gone",
                    pair.id, pair.rev.ts, prev_rev.ts
                )
            })?;
            collect_object_strings(&document.value().0, &mut strings);
            log_revisions += 1;
        }
    }

    Ok(KeepSet {
        strings,
        snapshot_documents,
        log_revisions,
        floor,
        latest_ts,
    })
}

/// The objects in a storage listing split by what the collector may do with
/// them.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OrphanSelection {
    /// Unreferenced objects old enough to delete, sorted by key.
    pub orphans: Vec<ObjectListing>,
    /// Objects whose key appears in the keep set.
    pub referenced: usize,
    /// Unreferenced objects younger than the minimum age (or with a
    /// modification time in the future).
    pub too_young: usize,
}

/// Split `listing` into referenced objects, unreferenced objects that are too
/// young to touch, and orphans.
///
/// `now` must have been read before the keep set was built: an object that
/// finished uploading after that instant may legitimately be missing from the
/// keep set, and is protected here purely by its age.
pub fn select_orphans(
    listing: Vec<ObjectListing>,
    keep: &BTreeSet<String>,
    now: SystemTime,
    min_age: Duration,
) -> OrphanSelection {
    let mut selection = OrphanSelection::default();
    for object in listing {
        if keep.contains(&*object.key) {
            selection.referenced += 1;
            continue;
        }
        let old_enough = match now.duration_since(object.last_modified) {
            Ok(age) => age >= min_age,
            // Modified in the future relative to our clock: skewed clocks or a
            // write in progress. Either way, not something to delete.
            Err(_) => false,
        };
        if !old_enough {
            selection.too_young += 1;
            continue;
        }
        selection.orphans.push(object);
    }
    selection.orphans.sort_by(|a, b| a.key.cmp(&b.key));
    selection
}

/// Whether the keep set describes the listing at all.
///
/// A healthy deployment with any search index has at least one listed object
/// that its metadata references. If storage holds objects but the keep set
/// protects none of them, the collector is almost certainly looking at the
/// wrong storage (or the metadata scan came back empty), and deleting would
/// destroy every segment. The one legitimate case — every index deleted long
/// ago, only orphans left — is not worth that risk; such a store is reported
/// but never collected.
pub fn keep_set_covers_storage(listed: usize, referenced: usize) -> bool {
    listed == 0 || referenced > 0
}

/// Stable digest over the sorted orphan keys, so an operator can compare a
/// dry run against an independently computed orphan set without depending on
/// every per-key log line (only a sample is logged).
pub fn orphan_digest(orphans: &[ObjectListing]) -> String {
    let mut hasher = Sha256::new();
    for object in orphans {
        hasher.update(object.key.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().as_hex()
}

/// How a round ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundOutcome {
    /// Keep set and listing were computed; deletes (if any) were attempted.
    Completed,
    /// Storage holds objects but the keep set references none of them.
    Refused,
}

impl RoundOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Refused => "refused",
        }
    }
}

/// What one collection round did.
#[derive(Debug)]
pub struct SearchSegmentGcSummary {
    pub mode: SearchSegmentGcMode,
    pub outcome: RoundOutcome,
    pub listed: usize,
    pub referenced: usize,
    pub too_young: usize,
    pub orphans: usize,
    pub orphan_bytes: u64,
    pub deleted: usize,
    pub deleted_bytes: u64,
    pub failed: usize,
    /// Orphans left for a later round because of the per-round delete cap.
    pub deferred: usize,
}

/// Run one collection round.
///
/// `now` is the wall-clock instant used for the age check and must be read
/// before calling (see `select_orphans`).
pub async fn collect_orphaned_search_segments<RT: Runtime>(
    database: &Database<RT>,
    search_storage: &Arc<dyn Storage>,
    rate_limiter: &RateLimiter<RT>,
    config: SearchSegmentGcConfig,
    now: SystemTime,
) -> anyhow::Result<SearchSegmentGcSummary> {
    let mode = config.mode;
    anyhow::ensure!(
        mode != SearchSegmentGcMode::Off,
        "collect_orphaned_search_segments called with mode off"
    );
    let _timer = search_segment_gc_timer();
    let keep = search_segment_keep_set(database, rate_limiter).await?;
    let listing = search_storage.list_objects("").await?;
    let listed = listing.len();
    let selection = select_orphans(listing, &keep.strings, now, config.min_object_age);
    let outcome = if keep_set_covers_storage(listed, selection.referenced) {
        RoundOutcome::Completed
    } else {
        RoundOutcome::Refused
    };

    let orphan_bytes: u64 = selection.orphans.iter().map(|o| o.size).sum();
    let digest = orphan_digest(&selection.orphans);
    for object in selection.orphans.iter().take(LOGGED_KEY_SAMPLE) {
        let age = now
            .duration_since(object.last_modified)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        tracing::info!(
            "search_segment_gc orphan key={} size={} age_seconds={age}",
            &*object.key,
            object.size
        );
    }

    let mut deleted = 0;
    let mut deleted_bytes = 0;
    let mut failed = 0;
    let mut deferred = 0;
    match (mode, outcome) {
        (SearchSegmentGcMode::Off, _) => unreachable!("mode off was rejected above"),
        (SearchSegmentGcMode::DryRun, _) | (SearchSegmentGcMode::Delete, RoundOutcome::Refused) => {
        },
        (SearchSegmentGcMode::Delete, RoundOutcome::Completed) => {
            for object in &selection.orphans {
                if deleted + failed >= config.max_deletes_per_round {
                    deferred += 1;
                    continue;
                }
                match search_storage.delete_object(&object.key).await {
                    Ok(()) => {
                        deleted += 1;
                        deleted_bytes += object.size;
                    },
                    Err(e) => {
                        tracing::error!(
                            "search_segment_gc failed to delete key={}: {e:#}",
                            &*object.key
                        );
                        failed += 1;
                    },
                }
            }
        },
    }

    let summary = SearchSegmentGcSummary {
        mode,
        outcome,
        listed,
        referenced: selection.referenced,
        too_young: selection.too_young,
        orphans: selection.orphans.len(),
        orphan_bytes,
        deleted,
        deleted_bytes,
        failed,
        deferred,
    };
    log_search_segment_gc_objects("orphaned", summary.orphans as u64);
    log_search_segment_gc_objects("deleted", summary.deleted as u64);
    log_search_segment_gc_objects("failed", summary.failed as u64);
    log_search_segment_gc_round(outcome.as_str());
    if outcome == RoundOutcome::Refused {
        tracing::warn!(
            "Search segment GC refused to collect: {listed} objects listed but none is referenced \
             by any retained _index revision. Search storage and index metadata do not match; \
             nothing was deleted."
        );
    }
    tracing::info!(
        "Search segment GC ({mode}, {outcome}): {listed} objects listed, {referenced} referenced \
         by a retained _index revision, {too_young} unreferenced but younger than \
         {min_age_secs}s, {orphans} orphaned ({orphan_bytes} bytes, digest {digest}); deleted \
         {deleted} ({deleted_bytes} bytes), failed {failed}, deferred {deferred}. Keep set: \
         {keep_strings} strings from {snapshot_documents} _index documents at {latest_ts} plus \
         {log_revisions} log revisions since {floor}.",
        mode = summary.mode,
        outcome = summary.outcome.as_str(),
        listed = summary.listed,
        referenced = summary.referenced,
        too_young = summary.too_young,
        min_age_secs = config.min_object_age.as_secs(),
        orphans = summary.orphans,
        orphan_bytes = summary.orphan_bytes,
        deleted = summary.deleted,
        deleted_bytes = summary.deleted_bytes,
        failed = summary.failed,
        deferred = summary.deferred,
        keep_strings = keep.strings.len(),
        snapshot_documents = keep.snapshot_documents,
        latest_ts = *keep.latest_ts,
        log_revisions = keep.log_revisions,
        floor = *keep.floor,
    );
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{
            BTreeMap,
            BTreeSet,
        },
        time::{
            Duration,
            SystemTime,
        },
    };

    use common::types::ObjectKey;
    use storage::ObjectListing;
    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
    };

    use super::{
        collect_object_strings,
        keep_set_covers_storage,
        orphan_digest,
        select_orphans,
        SearchSegmentGcConfig,
        SearchSegmentGcMode,
        MIN_OBJECT_AGE_FLOOR,
    };

    fn key(s: &str) -> ObjectKey {
        ObjectKey::try_from(s).unwrap()
    }

    fn listing(k: &str, age: Duration, now: SystemTime) -> ObjectListing {
        ObjectListing {
            key: key(k),
            last_modified: now - age,
            size: 10,
        }
    }

    #[test]
    fn mode_parses_exactly_three_values_and_rejects_the_rest() {
        assert_eq!(
            "off".parse::<SearchSegmentGcMode>().unwrap(),
            SearchSegmentGcMode::Off
        );
        assert_eq!(
            "dry_run".parse::<SearchSegmentGcMode>().unwrap(),
            SearchSegmentGcMode::DryRun
        );
        assert_eq!(
            " delete\n".parse::<SearchSegmentGcMode>().unwrap(),
            SearchSegmentGcMode::Delete
        );
        // Neither an unknown word nor a case variant may silently map to a
        // mode; the caller treats a parse error as "do nothing".
        assert!("on".parse::<SearchSegmentGcMode>().is_err());
        assert!("DELETE".parse::<SearchSegmentGcMode>().is_err());
        assert!("".parse::<SearchSegmentGcMode>().is_err());
    }

    #[test]
    fn config_rejects_an_age_below_the_floor() {
        // The age is the only thing protecting an uploaded segment until its
        // _index revision commits, so the knob must not be able to remove it.
        assert!(SearchSegmentGcConfig::new("delete", Duration::ZERO, 1000).is_err());
        assert!(SearchSegmentGcConfig::new(
            "delete",
            MIN_OBJECT_AGE_FLOOR - Duration::from_secs(1),
            1000
        )
        .is_err());
        let config = SearchSegmentGcConfig::new("delete", MIN_OBJECT_AGE_FLOOR, 1000).unwrap();
        assert_eq!(config.min_object_age, MIN_OBJECT_AGE_FLOOR);
        // The default knob value is accepted.
        assert!(SearchSegmentGcConfig::new("off", Duration::from_secs(24 * 60 * 60), 1000).is_ok());
        // A bad mode is rejected regardless of the age.
        assert!(
            SearchSegmentGcConfig::new("yes", Duration::from_secs(24 * 60 * 60), 1000).is_err()
        );
    }

    fn object(fields: Vec<(&str, ConvexValue)>) -> ConvexObject {
        let fields: BTreeMap<FieldName, ConvexValue> = fields
            .into_iter()
            .map(|(k, v)| (k.parse().unwrap(), v))
            .collect();
        ConvexObject::try_from(fields).unwrap()
    }

    fn string(s: &str) -> ConvexValue {
        ConvexValue::try_from(s.to_owned()).unwrap()
    }

    #[test]
    fn collects_every_nested_string_and_nothing_else() {
        let nested = object(vec![
            ("id_tracker_key", string("tracker-1")),
            (
                "segments",
                ConvexValue::try_from(vec![
                    ConvexValue::Object(object(vec![("segment_key", string("seg-2"))])),
                    string("loose-string"),
                    ConvexValue::Null,
                ])
                .unwrap(),
            ),
        ]);
        let root = object(vec![
            ("segment_key", string("seg-1")),
            ("size", ConvexValue::from(42i64)),
            ("flag", ConvexValue::from(true)),
            ("nested", ConvexValue::Object(nested)),
        ]);
        let mut strings = BTreeSet::new();
        collect_object_strings(&root, &mut strings);
        let expected: BTreeSet<String> = ["seg-1", "tracker-1", "seg-2", "loose-string"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(strings, expected);
    }

    #[test]
    fn select_orphans_protects_referenced_and_young_objects() {
        let now = SystemTime::now();
        let min_age = Duration::from_secs(3600);
        let keep: BTreeSet<String> = ["referenced-old", "referenced-young"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let objects = vec![
            listing("orphan-old-b", Duration::from_secs(7200), now),
            listing("referenced-old", Duration::from_secs(7200), now),
            listing("orphan-young", Duration::from_secs(60), now),
            listing("referenced-young", Duration::from_secs(60), now),
            listing("orphan-old-a", Duration::from_secs(7200), now),
            // Exactly at the age floor counts as old enough.
            listing("orphan-boundary", min_age, now),
        ];
        let selection = select_orphans(objects, &keep, now, min_age);
        assert_eq!(selection.referenced, 2);
        assert_eq!(selection.too_young, 1);
        let orphan_keys: Vec<&str> = selection.orphans.iter().map(|o| &*o.key).collect();
        assert_eq!(
            orphan_keys,
            vec!["orphan-boundary", "orphan-old-a", "orphan-old-b"]
        );
    }

    #[test]
    fn select_orphans_never_deletes_objects_modified_in_the_future() {
        let now = SystemTime::now();
        let keep = BTreeSet::new();
        let future = ObjectListing {
            key: key("from-the-future"),
            last_modified: now + Duration::from_secs(5),
            size: 1,
        };
        let selection = select_orphans(vec![future], &keep, now, Duration::from_secs(1));
        assert!(selection.orphans.is_empty());
        assert_eq!(selection.too_young, 1);
    }

    #[test]
    fn keep_set_must_cover_a_non_empty_listing() {
        // Nothing in storage: nothing to protect.
        assert!(keep_set_covers_storage(0, 0));
        // Storage has objects and metadata references at least one of them.
        assert!(keep_set_covers_storage(10, 1));
        // Storage has objects but metadata references none of them: the
        // storage and the metadata do not describe each other; refuse.
        assert!(!keep_set_covers_storage(10, 0));
    }

    #[test]
    fn orphan_digest_depends_only_on_the_sorted_keys() {
        let now = SystemTime::now();
        let a = vec![
            listing("k1", Duration::ZERO, now),
            listing("k2", Duration::ZERO, now),
        ];
        let mut b = vec![
            ObjectListing {
                key: key("k1"),
                last_modified: now - Duration::from_secs(99),
                size: 12345,
            },
            listing("k2", Duration::ZERO, now),
        ];
        assert_eq!(orphan_digest(&a), orphan_digest(&b));
        b.push(listing("k3", Duration::ZERO, now));
        assert_ne!(orphan_digest(&a), orphan_digest(&b));
        assert_eq!(orphan_digest(&[]), orphan_digest(&[]));
    }
}
