use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
    time::Duration,
};

use alloy::primitives::Address;
use log::{error, info, warn};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, sleep},
};
use utils::{EventBatch, SnapshotConfig, get_visor_path, process_rmp_file};

use crate::{
    FeatureSet,
    listeners::order_book::state::OrderBookState,
    metrics::{
        BBO_BROADCAST_LATENCY, EVENT_PROCESSING_LATENCY, EVENTS_PROCESSED_TOTAL, FILE_EVENTS_TOTAL,
        FILE_LINES_PARSED_TOTAL, L2_BROADCAST_LATENCY, L2_FLUSH_PHASE_LATENCY, ORDERBOOK_COINS_COUNT, ORDERBOOK_HEIGHT,
        ORDERBOOK_ORDERS_TOTAL, ORDERBOOK_TIME_MS, PARSE_ERRORS_TOTAL, PENDING_DIFFS_CACHE, PENDING_ORDERS_CACHE,
    },
    order_book::{
        Coin, Px, RawBbo, Snapshot, Sz,
        multi_book::{SnapshotHeightSource, Snapshots, load_snapshots_from_cli_json_at_height, read_visor_height},
    },
    prelude::*,
    types::{
        L2Book, L4Order, Stats,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::DEFAULT_LEVELS,
    },
};

mod parallel;
mod state;
mod utils;

const L2_BROADCAST_THROTTLE_MS: u64 = 50;
const L2_FLUSH_TICK_MS: u64 = 10;
const ALLBBO_BROADCAST_THROTTLE_MS: u64 = 50;
// Replay is journaled to a temporary file instead of retaining a second copy of
// every JSON line in RAM. These are disk-safety limits, not memory budgets.
const SNAPSHOT_REPLAY_REFRESH_MAX_LINES: usize = 10_000_000;
const SNAPSHOT_REPLAY_REFRESH_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;
const SNAPSHOT_REPLAY_INITIAL_MAX_LINES: usize = 20_000_000;
const SNAPSHOT_REPLAY_INITIAL_MAX_BYTES: usize = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotTaskKind {
    Initial,
    Refresh,
}

struct SnapshotTaskResult {
    kind: SnapshotTaskKind,
    result: Result<SnapshotInstallOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotInstallOutcome {
    Installed { replayed_lines: usize },
    SkippedReplayOverflow { cached_lines: usize, cached_bytes: usize },
    SkippedStaleSnapshot { snapshot_height: u64, replay_cutoff_height: u64, started_book_height: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotReplayLimits {
    max_lines: usize,
    max_bytes: usize,
}

impl SnapshotReplayLimits {
    fn for_kind(kind: SnapshotTaskKind) -> Self {
        match kind {
            SnapshotTaskKind::Initial => {
                Self { max_lines: SNAPSHOT_REPLAY_INITIAL_MAX_LINES, max_bytes: SNAPSHOT_REPLAY_INITIAL_MAX_BYTES }
            }
            SnapshotTaskKind::Refresh => {
                Self { max_lines: SNAPSHOT_REPLAY_REFRESH_MAX_LINES, max_bytes: SNAPSHOT_REPLAY_REFRESH_MAX_BYTES }
            }
        }
    }
}

impl Default for SnapshotReplayLimits {
    fn default() -> Self {
        Self::for_kind(SnapshotTaskKind::Refresh)
    }
}

#[derive(Debug, Default)]
struct SnapshotReplayCache {
    writer: Option<BufWriter<File>>,
    journal_path: Option<PathBuf>,
    line_count: usize,
    total_bytes: usize,
    overflowed: bool,
    failure_reason: Option<String>,
    started_book_height: u64,
    limits: SnapshotReplayLimits,
}

impl SnapshotReplayCache {
    fn new(started_book_height: u64, kind: SnapshotTaskKind) -> std::io::Result<Self> {
        let (file, journal_path) = create_replay_journal()?;
        Ok(Self {
            writer: Some(BufWriter::new(file)),
            journal_path,
            line_count: 0,
            total_bytes: 0,
            overflowed: false,
            failure_reason: None,
            started_book_height,
            limits: SnapshotReplayLimits::for_kind(kind),
        })
    }

    fn push(&mut self, event_source: EventSource, line: &str) -> Option<String> {
        if self.overflowed {
            return None;
        }

        let next_lines = self.line_count.saturating_add(1);
        let next_bytes = self.total_bytes.saturating_add(line.len());
        if next_lines > self.limits.max_lines || next_bytes > self.limits.max_bytes {
            self.overflowed = true;
            self.failure_reason = Some(format!(
                "replay journal exceeded its limits of {} lines / {} bytes",
                self.limits.max_lines, self.limits.max_bytes
            ));
            return self.failure_reason.clone();
        }

        let tag = match event_source {
            EventSource::OrderStatuses => b'S',
            EventSource::OrderDiffs => b'D',
            EventSource::Fills => return None,
        };
        let write_result = self.writer.as_mut().map_or_else(
            || Err(std::io::Error::other("replay journal is unavailable")),
            |writer| {
                writer
                    .write_all(&[tag])
                    .and_then(|()| writer.write_all(line.as_bytes()))
                    .and_then(|()| writer.write_all(b"\n"))
            },
        );
        if let Err(err) = write_result {
            self.overflowed = true;
            self.failure_reason = Some(format!("could not write replay journal: {err}"));
            return self.failure_reason.clone();
        }

        self.line_count = next_lines;
        self.total_bytes = next_bytes;
        None
    }
}

impl Drop for SnapshotReplayCache {
    fn drop(&mut self) {
        // Close the file before unlinking it so cleanup also works on platforms
        // that do not allow deleting an open file.
        drop(self.writer.take());
        if let Some(path) = self.journal_path.take() {
            drop(fs::remove_file(path));
        }
    }
}

fn create_replay_journal() -> std::io::Result<(File, Option<PathBuf>)> {
    static NEXT_REPLAY_JOURNAL_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    for _ in 0..100 {
        let id = NEXT_REPLAY_JOURNAL_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = std::env::temp_dir().join(format!("orderbook-server-replay-{}-{id}.journal", std::process::id()));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => {
                // Unix keeps the inode alive through the open file descriptor, so
                // unlink immediately to avoid stale multi-GB journals after SIGKILL.
                #[cfg(unix)]
                {
                    fs::remove_file(&path)?;
                    return Ok((file, None));
                }
                #[cfg(not(unix))]
                return Ok((file, Some(path)));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }

    Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "could not allocate a unique snapshot replay journal"))
}

#[derive(Default)]
pub(crate) struct AllBboSubscriptionRegistry {
    active: AtomicUsize,
}

impl AllBboSubscriptionRegistry {
    pub(crate) fn register(&self) {
        self.active.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn unregister(&self) {
        let mut current = self.active.load(AtomicOrdering::Relaxed);
        while current > 0 {
            match self.active.compare_exchange_weak(
                current,
                current - 1,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    pub(crate) fn has_active(&self) -> bool {
        self.active.load(AtomicOrdering::Relaxed) > 0
    }
}

#[derive(Default)]
pub(crate) struct L2SubscriptionRegistry {
    params: StdMutex<HashMap<L2SnapshotParams, usize>>,
    coins: StdMutex<HashMap<Coin, usize>>,
    keys: StdMutex<HashMap<L2SubscriptionKey, usize>>,
}

impl L2SubscriptionRegistry {
    fn params(&self) -> StdMutexGuard<'_, HashMap<L2SnapshotParams, usize>> {
        self.params.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn coins(&self) -> StdMutexGuard<'_, HashMap<Coin, usize>> {
        self.coins.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn keys(&self) -> StdMutexGuard<'_, HashMap<L2SubscriptionKey, usize>> {
        self.keys.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn register_l2(&self, key: L2SubscriptionKey) {
        *self.params().entry(key.params).or_insert(0) += 1;
        *self.coins().entry(key.coin.clone()).or_insert(0) += 1;
        *self.keys().entry(key).or_insert(0) += 1;
    }

    pub(crate) fn unregister_l2(&self, key: &L2SubscriptionKey) {
        let mut counts = self.params();
        if let Some(count) = counts.get_mut(&key.params) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&key.params);
            }
        }
        drop(counts);

        let mut counts = self.coins();
        if let Some(count) = counts.get_mut(&key.coin) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&key.coin);
            }
        }
        drop(counts);

        let mut counts = self.keys();
        if let Some(count) = counts.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                counts.remove(key);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn active_params(&self) -> HashSet<L2SnapshotParams> {
        self.params().keys().copied().collect()
    }

    #[cfg(test)]
    pub(crate) fn active_coins(&self) -> HashSet<Coin> {
        self.coins().keys().cloned().collect()
    }

    pub(crate) fn active_keys(&self) -> HashSet<L2SubscriptionKey> {
        self.keys().keys().cloned().collect()
    }
}

fn snapshot_refresh_interval(hours: u64) -> Option<Duration> {
    (hours > 0).then(|| Duration::from_secs(hours.saturating_mul(60 * 60)))
}

fn next_snapshot_trigger(
    features: FeatureSet,
    is_ready: bool,
    snapshot_fetch_pending: bool,
    next_refresh_at: Option<Instant>,
    now: Instant,
) -> Option<SnapshotTaskKind> {
    if !features.requires_book_state() || snapshot_fetch_pending {
        return None;
    }
    if !is_ready {
        return Some(SnapshotTaskKind::Initial);
    }
    if next_refresh_at.is_some_and(|refresh_at| now >= refresh_at) {
        return Some(SnapshotTaskKind::Refresh);
    }
    None
}

fn snapshot_replay_cutoff_height(
    height_source: SnapshotHeightSource,
    snapshot_height: u64,
    visor_height_before: Result<u64>,
    visor_height_after: Result<u64>,
) -> Result<(u64, Option<(u64, u64)>)> {
    match height_source {
        SnapshotHeightSource::Embedded => Ok((snapshot_height, None)),
        SnapshotHeightSource::Visor => {
            let before = visor_height_before.map_err(|err| -> Error {
                format!("Failed to read visor height before bare snapshot generation: {err}").into()
            })?;
            let after = visor_height_after.map_err(|err| -> Error {
                format!("Failed to read visor height after bare snapshot generation: {err}").into()
            })?;
            Ok((before.min(after), Some((before, after))))
        }
    }
}

fn fetch_snapshot(
    kind: SnapshotTaskKind,
    snapshot_config: SnapshotConfig,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<SnapshotTaskResult>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res: Result<SnapshotInstallOutcome> = async {
            // Start replay capture before generating the snapshot so the new state can
            // catch up to events written while hl-node is reading abci_state.rmp.
            listener.lock().await.begin_snapshot_replay_for(kind)?;

            let visor_path = get_visor_path(&snapshot_config);
            let visor_height_before = read_visor_height(&visor_path).await;
            let output_fln = process_rmp_file(&snapshot_config).await?;
            let visor_height_after = read_visor_height(&visor_path).await;
            let fallback_height =
                visor_height_after.as_ref().or(visor_height_before.as_ref()).copied().unwrap_or_default();
            let loaded_snapshot = load_snapshots_from_cli_json_at_height::<InnerL4Order, (Address, L4Order)>(
                &output_fln,
                fallback_height,
            )
            .await?;
            let height_source = loaded_snapshot.height_source;
            let height = loaded_snapshot.height;
            let (replay_cutoff_height, visor_heights) =
                snapshot_replay_cutoff_height(height_source, height, visor_height_before, visor_height_after)?;
            if let Some((visor_height_before, visor_height_after)) =
                visor_heights.filter(|(before, after)| before != after)
            {
                warn!(
                    "Bare snapshot output used visor height, and visor changed from {visor_height_before} to \
                     {visor_height_after} while hl-node generated the snapshot; using {replay_cutoff_height} as \
                     conservative replay cutoff"
                );
            }
            info!("Snapshot fetched at height {height} ({height_source:?}); replay cutoff {replay_cutoff_height}");
            // Give file watchers a short window to deliver any writes that landed
            // around the snapshot read before we close replay capture.
            sleep(Duration::from_secs(1)).await;

            let new_order_book = OrderBookState::from_snapshot(loaded_snapshot.snapshots, height, 0, true, ignore_spot);
            let outcome = listener.lock().await.install_snapshot_state_with_replay_cutoff(
                new_order_book,
                height,
                replay_cutoff_height,
                kind,
            )?;
            Ok(outcome)
        }
        .await;

        if res.is_err() {
            listener.lock().await.clear_snapshot_replay();
        }
        let _unused = tx.send(SnapshotTaskResult { kind, result: res });
        Ok::<(), Error>(())
    });
}

fn replay_snapshot_cache(
    state: &mut OrderBookState,
    snapshot_height: u64,
    mut replay_cache: SnapshotReplayCache,
) -> Result<usize> {
    let Some(writer) = replay_cache.writer.take() else {
        if replay_cache.line_count == 0 {
            return Ok(0);
        }
        return Err("Snapshot replay journal was unavailable".into());
    };
    let mut file = writer.into_inner().map_err(std::io::IntoInnerError::into_error)?;
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut replayed_lines = 0;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let Some((tag, json)) = line.as_bytes().split_first() else { continue };
        let json = std::str::from_utf8(json)?.trim_end_matches(['\r', '\n']);
        match tag {
            b'S' => match sonic_rs::from_str::<Batch<NodeDataOrderStatus>>(json) {
                Ok(batch) => {
                    if batch.block_number() <= snapshot_height {
                        continue;
                    }
                    if let Err(err) = state.apply_order_statuses_hft(batch) {
                        warn!(
                            "Skipping cached order-status replay line above snapshot height {snapshot_height}: {err}"
                        );
                    }
                    replayed_lines += 1;
                }
                Err(err) => {
                    warn!("Skipping unparsable cached order-status replay line: {err}");
                }
            },
            b'D' => match sonic_rs::from_str::<Batch<NodeDataOrderDiff>>(json) {
                Ok(batch) => {
                    if batch.block_number() <= snapshot_height {
                        continue;
                    }
                    if let Err(err) = state.replay_order_diffs_hft(batch) {
                        warn!("Skipping cached order-diff replay line above snapshot height {snapshot_height}: {err}");
                    }
                    replayed_lines += 1;
                }
                Err(err) => {
                    warn!("Skipping unparsable cached order-diff replay line: {err}");
                }
            },
            _ => warn!("Skipping replay journal line with unknown source tag {tag}"),
        }
    }
    Ok(replayed_lines)
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    features: FeatureSet,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    // Stream batches at or below this height are already represented by the
    // latest installed snapshot and must not be applied from queued file events.
    stream_ignore_through_height: u64,
    // Stream order-diff batches above `stream_ignore_through_height` but at or
    // below this height may already be represented by a bare snapshot. Apply
    // them with replay semantics so stale updates cannot roll the snapshot back.
    stream_replay_guard_through_height: u64,
    // Only Some while a snapshot task is running and needs live stream replay.
    snapshot_replay_cache: Option<SnapshotReplayCache>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    // Throttle L2 broadcasts to prevent flooding clients
    last_l2_broadcast: Option<Instant>,
    // Coins whose L2 snapshot cache is stale. This intentionally survives the
    // 50ms L2 throttle window; otherwise changes suppressed by the throttle are
    // never recomputed and subscribers can see stale L2 while BBO is current.
    pending_l2_changed_coins: HashSet<Coin>,
    // Incremental L2 snapshot cache. Each per-coin entry is Arc'd and shared with
    // the broadcast Arc, so unchanged coins cost an atomic bump rather than a
    // full level-vector clone. Invalidated in `init_from_snapshot`.
    l2_snapshot_cache: HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    l2_prepared_hashes: HashMap<L2SubscriptionKey, u64>,
    next_l2_version: u64,
    l2_generation: u64,
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    last_allbbo_broadcast: Option<Instant>,
    pending_allbbo_changed_coins: HashSet<Coin>,
    allbbo_subscription_registry: Arc<AllBboSubscriptionRegistry>,
    last_universe: HashSet<Coin>,
    stats: StatsAccumulator,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        features: FeatureSet,
    ) -> Self {
        Self {
            ignore_spot,
            features,
            order_book_state: None,
            stream_ignore_through_height: 0,
            stream_replay_guard_through_height: 0,
            snapshot_replay_cache: None,
            internal_message_tx,
            last_l2_broadcast: None,
            pending_l2_changed_coins: HashSet::new(),
            l2_snapshot_cache: HashMap::new(),
            l2_prepared_hashes: HashMap::new(),
            next_l2_version: 1,
            l2_generation: 0,
            l2_subscription_registry: Arc::new(L2SubscriptionRegistry::default()),
            last_allbbo_broadcast: None,
            pending_allbbo_changed_coins: HashSet::new(),
            allbbo_subscription_registry: Arc::new(AllBboSubscriptionRegistry::default()),
            last_universe: HashSet::new(),
            stats: StatsAccumulator::default(),
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.features.requires_book_state() || self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    pub(crate) fn l2_subscription_registry(&self) -> Arc<L2SubscriptionRegistry> {
        Arc::clone(&self.l2_subscription_registry)
    }

    pub(crate) fn allbbo_subscription_registry(&self) -> Arc<AllBboSubscriptionRegistry> {
        Arc::clone(&self.allbbo_subscription_registry)
    }

    #[cfg(test)]
    fn begin_snapshot_replay(&mut self) {
        self.begin_snapshot_replay_for(SnapshotTaskKind::Refresh).expect("temporary replay journal should open");
    }

    fn begin_snapshot_replay_for(&mut self, kind: SnapshotTaskKind) -> Result<()> {
        let started_book_height = self.order_book_state.as_ref().map_or(0, OrderBookState::height);
        self.snapshot_replay_cache = Some(SnapshotReplayCache::new(started_book_height, kind)?);
        Ok(())
    }

    fn clear_snapshot_replay(&mut self) {
        self.snapshot_replay_cache = None;
    }

    fn record_snapshot_replay_line(&mut self, event_source: EventSource, line: &str) {
        if !matches!(event_source, EventSource::OrderStatuses | EventSource::OrderDiffs) {
            return;
        }

        let Some(cache) = &mut self.snapshot_replay_cache else { return };
        if let Some(reason) = cache.push(event_source, line) {
            warn!(
                "Snapshot replay journal failed at {} lines / {} bytes: {}; this snapshot will not be installed",
                cache.line_count, cache.total_bytes, reason
            );
        }
    }

    #[cfg(test)]
    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("Initializing from snapshot at height {}", height);
        let new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        self.replace_order_book_state(new_order_book, height, height, true);
        self.snapshot_replay_cache = None;
        info!("Order book ready at height {}", height);
    }

    fn replace_order_book_state(
        &mut self,
        new_order_book: OrderBookState,
        replay_cutoff_height: u64,
        snapshot_height: u64,
        reset_l2_version: bool,
    ) {
        self.order_book_state = Some(new_order_book);
        self.stream_ignore_through_height = self.stream_ignore_through_height.max(replay_cutoff_height);
        self.stream_replay_guard_through_height = snapshot_height;
        self.last_universe = self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe);
        // The incremental L2 cache references the previous book's coins/levels;
        // drop it so the next broadcast does a full rebuild against the new state.
        self.l2_snapshot_cache = HashMap::new();
        self.l2_prepared_hashes.clear();
        if reset_l2_version {
            self.next_l2_version = 1;
        }
        self.l2_generation = self.l2_generation.wrapping_add(1);
        self.pending_l2_changed_coins.clear();
        self.pending_allbbo_changed_coins.clear();
    }

    #[cfg(test)]
    fn install_snapshot_state(
        &mut self,
        new_order_book: OrderBookState,
        snapshot_height: u64,
        kind: SnapshotTaskKind,
    ) -> Result<SnapshotInstallOutcome> {
        self.install_snapshot_state_with_replay_cutoff(new_order_book, snapshot_height, snapshot_height, kind)
    }

    fn install_snapshot_state_with_replay_cutoff(
        &mut self,
        mut new_order_book: OrderBookState,
        snapshot_height: u64,
        replay_cutoff_height: u64,
        kind: SnapshotTaskKind,
    ) -> Result<SnapshotInstallOutcome> {
        let replay_cache = self.snapshot_replay_cache.take().unwrap_or_default();
        if replay_cache.overflowed {
            let cached_lines = replay_cache.line_count;
            let cached_bytes = replay_cache.total_bytes;
            let failure_reason = replay_cache.failure_reason.as_deref().unwrap_or("unknown replay journal failure");
            if kind == SnapshotTaskKind::Refresh {
                warn!(
                    "Skipping background snapshot refresh at height {snapshot_height}: replay journal failed \
                     ({cached_lines} lines / {cached_bytes} bytes): {failure_reason}"
                );
                return Ok(SnapshotInstallOutcome::SkippedReplayOverflow { cached_lines, cached_bytes });
            }
            return Err(format!(
                "Snapshot replay journal failed during initial snapshot ({cached_lines} lines / {cached_bytes} bytes): \
                 {failure_reason}"
            )
            .into());
        }

        if kind == SnapshotTaskKind::Refresh && replay_cutoff_height < replay_cache.started_book_height {
            warn!(
                "Skipping background snapshot refresh at height {snapshot_height}: replay cutoff {replay_cutoff_height} \
                 was behind current book height {} when replay capture started",
                replay_cache.started_book_height
            );
            return Ok(SnapshotInstallOutcome::SkippedStaleSnapshot {
                snapshot_height,
                replay_cutoff_height,
                started_book_height: replay_cache.started_book_height,
            });
        }

        let current_time = (kind == SnapshotTaskKind::Refresh)
            .then(|| self.order_book_state.as_ref().map(OrderBookState::time))
            .flatten();
        let replayed_lines = replay_snapshot_cache(&mut new_order_book, replay_cutoff_height, replay_cache)?;
        if let Some(current_time) = current_time {
            if new_order_book.time() < current_time {
                new_order_book.set_time(current_time);
            }
        }
        let old_universe = self.last_universe.clone();
        let reset_l2_version = kind == SnapshotTaskKind::Initial;
        self.replace_order_book_state(new_order_book, replay_cutoff_height, snapshot_height, reset_l2_version);
        self.broadcast_universe_after_snapshot(old_universe);
        self.force_snapshot_refresh_updates();
        info!(
            "Order book ready at height {snapshot_height}; replay cutoff {replay_cutoff_height}; replayed \
             {replayed_lines} cached stream lines"
        );
        Ok(SnapshotInstallOutcome::Installed { replayed_lines })
    }

    fn should_ignore_stream_batch(&self, event_source: EventSource, height: u64) -> bool {
        matches!(event_source, EventSource::OrderStatuses | EventSource::OrderDiffs)
            && self.features.requires_book_state()
            && height <= self.stream_ignore_through_height
    }

    fn should_replay_guard_stream_batch(&self, event_source: EventSource, height: u64) -> bool {
        matches!(event_source, EventSource::OrderDiffs)
            && self.features.requires_book_state()
            && height <= self.stream_replay_guard_through_height
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    pub(crate) fn all_bbos(&self) -> Option<(u64, Vec<(Coin, RawBbo)>)> {
        self.order_book_state.as_ref().map(OrderBookState::get_all_bbos)
    }

    fn broadcast_universe_after_snapshot(&mut self, old_universe: HashSet<Coin>) {
        let Some(state) = &self.order_book_state else { return };
        let universe = state.compute_universe();
        self.last_universe = universe.clone();
        if universe == old_universe {
            return;
        }

        if let Some(tx) = &self.internal_message_tx {
            if tx.receiver_count() > 0 {
                drop(tx.send(Arc::new(InternalMessage::Universe { universe })));
            }
        }
    }

    fn force_snapshot_refresh_updates(&mut self) {
        let Some(tx) = self.internal_message_tx.as_ref().cloned() else { return };
        if tx.receiver_count() == 0 {
            return;
        }

        let Some(state) = &self.order_book_state else { return };
        let universe = state.compute_universe();

        if self.features.bbo() && !universe.is_empty() {
            let (time, bbos) = state.get_bbos_for_coins(&universe);
            if !bbos.is_empty() {
                drop(tx.send(Arc::new(InternalMessage::BboUpdate { bbos, time })));
            }
        }

        if self.features.allbbo() && self.allbbo_subscription_registry.has_active() {
            let (time, bbos) = state.get_all_bbos();
            if !bbos.is_empty() {
                self.last_allbbo_broadcast = Some(Instant::now());
                drop(tx.send(Arc::new(InternalMessage::AllBboUpdate { bbos, time })));
            }
        }

        if self.features.l2book() {
            let active_keys = self.l2_subscription_registry.active_keys();
            if !active_keys.is_empty() {
                self.pending_l2_changed_coins.extend(active_keys.into_iter().map(|key| key.coin));
                self.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
            }
        }
    }

    fn broadcast_universe_if_changed(&mut self, changed_coins: &HashSet<Coin>) {
        let Some(state) = &self.order_book_state else { return };
        let might_have_changed = state.coin_count() != self.last_universe.len()
            || changed_coins.iter().any(|coin| !self.last_universe.contains(coin));
        if !might_have_changed {
            return;
        }

        let universe = state.compute_universe();
        if universe == self.last_universe {
            return;
        }

        self.last_universe = universe.clone();
        if let Some(tx) = &self.internal_message_tx {
            if tx.receiver_count() > 0 {
                drop(tx.send(Arc::new(InternalMessage::Universe { universe })));
            }
        }
    }

    #[cfg(test)]
    fn flush_l2_if_due(&mut self) {
        if let Some(job) = self.take_l2_flush_job_if_due() {
            let prepared = job.prepare();
            self.finish_l2_flush(prepared);
        }
    }

    fn take_l2_flush_job_if_due(&mut self) -> Option<L2FlushJob> {
        if !self.features.l2book() {
            return None;
        }

        // Throttled L2 snapshot broadcast for L2Book subscribers.
        // l2_snapshots_uncached() walks every coin x every aggregation variant, so
        // limit to 20 broadcasts/sec max (50ms). Skip entirely when no coin changed
        // since the previous L2 compute - there's nothing new to send and the
        // per-client dedup would drop it anyway.
        // (Heartbeat resend for quiet coins is handled per-connection in handle_socket.)
        //
        // CRITICAL: the receiver_count gate must wrap l2_snapshots_uncached(), not
        // sit between compute and send. A prior version updated last_l2_broadcast
        // only when receivers existed, so with zero subscribers the throttle reset
        // never fired and the par_iter ran on every event - tens of GB of allocator
        // churn per hour and a pinned listener mutex.
        if self.pending_l2_changed_coins.is_empty() {
            return None;
        }

        let active_keys = self.l2_subscription_registry.active_keys();
        if active_keys.is_empty() {
            self.pending_l2_changed_coins.clear();
            self.l2_snapshot_cache.clear();
            self.l2_prepared_hashes.clear();
            self.next_l2_version = 1;
            return None;
        }
        let requested_params: HashSet<L2SnapshotParams> = active_keys.iter().map(|key| key.params).collect();
        let active_coins: HashSet<Coin> = active_keys.iter().map(|key| key.coin.clone()).collect();
        if !self.pending_l2_changed_coins.iter().any(|coin| active_coins.contains(coin)) {
            self.pending_l2_changed_coins.clear();
            return None;
        }

        let should_broadcast_l2 = !self.pending_l2_changed_coins.is_empty()
            && self
                .last_l2_broadcast
                .map(|t| t.elapsed() >= Duration::from_millis(L2_BROADCAST_THROTTLE_MS))
                .unwrap_or(true);

        if !should_broadcast_l2 {
            return None;
        }

        let tx = self.internal_message_tx.as_ref()?;
        let has_receivers = tx.receiver_count() > 0;
        // Mark the throttle as fired regardless of receivers so we don't
        // re-check on every subsequent event when nobody is listening.
        self.last_l2_broadcast = Some(Instant::now());
        if !has_receivers {
            return None;
        }

        let state = self.order_book_state.as_ref()?;
        let l2_start = Instant::now();
        let mut changed_for_l2 = std::mem::take(&mut self.pending_l2_changed_coins);
        changed_for_l2.retain(|coin| active_coins.contains(coin));
        let snapshot_start = Instant::now();
        let (time, l2_snapshots) =
            state.l2_snapshots_incremental(&changed_for_l2, &requested_params, &mut self.l2_snapshot_cache);
        L2_FLUSH_PHASE_LATENCY.with_label_values(&["snapshot"]).observe(snapshot_start.elapsed().as_secs_f64());

        static L2_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let bc = L2_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if bc % 100 == 0 {
            info!("L2 broadcast #{} at time {}", bc, time);
        }

        Some(L2FlushJob {
            generation: self.l2_generation,
            time,
            l2_snapshots,
            active_keys,
            tx: tx.clone(),
            prepared_hashes: std::mem::take(&mut self.l2_prepared_hashes),
            next_version: self.next_l2_version,
            started_at: l2_start,
        })
    }

    fn finish_l2_flush(&mut self, prepared: PreparedL2Flush) {
        if prepared.generation != self.l2_generation {
            return;
        }
        let publish_start = Instant::now();
        self.l2_prepared_hashes = prepared.prepared_hashes;
        self.next_l2_version = prepared.next_version;
        if !prepared.l2_books.is_empty() {
            let msg = Arc::new(InternalMessage::L2Update { l2_books: prepared.l2_books });
            drop(prepared.tx.send(msg));
        }
        L2_FLUSH_PHASE_LATENCY.with_label_values(&["publish"]).observe(publish_start.elapsed().as_secs_f64());
        L2_BROADCAST_LATENCY.observe(prepared.started_at.elapsed().as_secs_f64());
    }

    fn flush_allbbo_if_due(&mut self) {
        if let Some(msg) = self.take_allbbo_update_if_due() {
            if let Some(tx) = &self.internal_message_tx {
                drop(tx.send(Arc::new(msg)));
            }
        }
    }

    fn take_allbbo_update_if_due(&mut self) -> Option<InternalMessage> {
        if !self.features.allbbo() || self.pending_allbbo_changed_coins.is_empty() {
            return None;
        }

        if !self.allbbo_subscription_registry.has_active() {
            self.pending_allbbo_changed_coins.clear();
            return None;
        }

        let should_broadcast = self
            .last_allbbo_broadcast
            .map(|t| t.elapsed() >= Duration::from_millis(ALLBBO_BROADCAST_THROTTLE_MS))
            .unwrap_or(true);
        if !should_broadcast {
            return None;
        }

        let tx = self.internal_message_tx.as_ref()?;
        self.last_allbbo_broadcast = Some(Instant::now());
        if tx.receiver_count() == 0 {
            self.pending_allbbo_changed_coins.clear();
            return None;
        }

        let state = self.order_book_state.as_ref()?;
        let changed = std::mem::take(&mut self.pending_allbbo_changed_coins);
        let (time, bbos) = state.get_bbos_for_changed_coins(&changed);
        if bbos.is_empty() {
            return None;
        }

        Some(InternalMessage::AllBboUpdate { bbos, time })
    }

    fn flush_stats_if_due(&mut self) {
        if let Some(msg) = self.take_stats_update() {
            if let Some(tx) = &self.internal_message_tx {
                if tx.receiver_count() > 0 {
                    drop(tx.send(Arc::new(msg)));
                }
            }
        }
    }

    fn take_stats_update(&mut self) -> Option<InternalMessage> {
        if !self.features.stats() {
            return None;
        }
        let time = chrono::Utc::now().timestamp_millis().max(0) as u64;
        Some(InternalMessage::Stats { stats: self.stats.flush(time) })
    }
}

impl OrderBookListener {
    /// HFT version of process_data - doesn't skip first line errors since we're processing complete JSON lines
    pub(crate) fn process_data_hft(&mut self, line: String, event_source: EventSource) -> Result<()> {
        /// Largest batch we'll process. Each event is a few hundred bytes; a 100k-event
        /// batch would already block the listener for seconds and pin hundreds of MB.
        /// In normal operation a single block's batch is tens to low thousands of events.
        const MAX_EVENTS_PER_BATCH: usize = 100_000;
        // Count events for debugging
        static HFT_EVENT_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = HFT_EVENT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 1000 == 0 {
            info!("process_data_hft event #{}, source: {}, line_len: {}", count, event_source, line.len());
        }

        if line.is_empty() {
            return Ok(());
        }

        let source_enabled = match event_source {
            EventSource::Fills => self.features.watch_fills(),
            EventSource::OrderStatuses => self.features.watch_order_statuses(),
            EventSource::OrderDiffs => self.features.watch_order_diffs(),
        };
        if !source_enabled {
            return Ok(());
        }

        // Parse the batch
        let res = match event_source {
            EventSource::Fills => sonic_rs::from_str::<Batch<NodeDataFill>>(&line).map(|batch| {
                let height = batch.block_number();
                (height, EventBatch::Fills(batch))
            }),
            EventSource::OrderStatuses => sonic_rs::from_str(&line)
                .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
            EventSource::OrderDiffs => sonic_rs::from_str(&line)
                .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
        };

        let (height, event_batch) = match res {
            Ok(data) => data,
            Err(err) => {
                // Log ALL parse errors for debugging
                let err_source_label = match event_source {
                    EventSource::Fills => "fills",
                    EventSource::OrderStatuses => "orders",
                    EventSource::OrderDiffs => "diffs",
                };
                PARSE_ERRORS_TOTAL.with_label_values(&[err_source_label]).inc();
                static PARSE_ERR_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let err_count = PARSE_ERR_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if err_count % 1000 == 0 {
                    error!("parse error #{}: {}, source: {}, line_len: {}", err_count, err, event_source, line.len());
                }
                return Ok(()); // Skip this line but don't fail
            }
        };

        // Sanity cap on batch size. A malformed/malicious line could otherwise
        // pin hundreds of MB and freeze the listener for seconds.
        let events_len = match &event_batch {
            EventBatch::Orders(b) => b.events_len(),
            EventBatch::BookDiffs(b) => b.events_len(),
            EventBatch::Fills(b) => b.events_len(),
        };
        if events_len > MAX_EVENTS_PER_BATCH {
            let source_label = match event_source {
                EventSource::Fills => "fills",
                EventSource::OrderStatuses => "orders",
                EventSource::OrderDiffs => "diffs",
            };
            PARSE_ERRORS_TOTAL.with_label_values(&[source_label]).inc();
            error!(
                "Dropping oversize batch from {source_label}: {events_len} events (cap {MAX_EVENTS_PER_BATCH}), height={height}"
            );
            return Ok(());
        }
        let ignore_for_book_state = self.should_ignore_stream_batch(event_source, height);
        let replay_guard_for_book_state =
            !ignore_for_book_state && self.should_replay_guard_stream_batch(event_source, height);
        if !ignore_for_book_state {
            self.record_snapshot_replay_line(event_source, &line);
        }
        if self.features.stats() {
            self.stats.record(&event_batch, height);
        }

        // Log successful parses periodically
        static PARSE_OK_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let ok_count = PARSE_OK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Record file watcher metrics
        let source_label = match event_source {
            EventSource::Fills => "fills",
            EventSource::OrderStatuses => "orders",
            EventSource::OrderDiffs => "diffs",
        };
        FILE_EVENTS_TOTAL.with_label_values(&[source_label]).inc();
        FILE_LINES_PARSED_TOTAL.with_label_values(&[source_label]).inc_by(line.len() as u64);
        let process_start = Instant::now();

        if ok_count % 10_000 == 0 {
            info!("parse OK #{}: height={}, source={}", ok_count, height, event_source);
        }

        if height % 100 == 0 {
            info!("{event_source} block: {height}");
        }

        // HFT mode: Process events DIRECTLY without block-level synchronization
        // This is arbor's key insight - process independently with order-level caching
        let result = match event_batch {
            EventBatch::Orders(batch) => {
                let should_broadcast = self.features.l4book() || self.features.orderupdates();
                if should_broadcast {
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let msg = Arc::new(InternalMessage::L4OrderStatuses { batch: batch.clone() });
                            drop(tx.send(msg));
                        }
                    }
                }
                EVENTS_PROCESSED_TOTAL.with_label_values(&["orders"]).inc();
                if self.features.requires_book_state() && !ignore_for_book_state {
                    self.order_book_state
                        .as_mut()
                        .map_or_else(|| Ok(HashSet::new()), |state| state.apply_order_statuses_hft(batch))
                } else {
                    Ok(HashSet::new())
                }
            }
            EventBatch::BookDiffs(batch) => {
                let should_broadcast = self.features.l4book() || self.features.bookdiffs();
                if should_broadcast {
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let to_broadcast = if self.ignore_spot {
                                batch.filter_events(|d| !d.coin().is_spot())
                            } else {
                                batch.clone()
                            };
                            if to_broadcast.events_len() > 0 {
                                let msg = Arc::new(InternalMessage::L4OrderDiffs { batch: to_broadcast });
                                drop(tx.send(msg));
                            }
                        }
                    }
                }
                EVENTS_PROCESSED_TOTAL.with_label_values(&["diffs"]).inc();
                if self.features.requires_book_state() && !ignore_for_book_state {
                    self.order_book_state.as_mut().map_or_else(
                        || Ok(HashSet::new()),
                        |state| {
                            if replay_guard_for_book_state {
                                state.replay_order_diffs_hft(batch)
                            } else {
                                state.apply_order_diffs_hft(batch)
                            }
                        },
                    )
                } else {
                    Ok(HashSet::new())
                }
            }
            EventBatch::Fills(batch) => {
                EVENTS_PROCESSED_TOTAL.with_label_values(&["fills"]).inc();
                if self.features.trades() {
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            drop(tx.send(snapshot));
                        }
                    }
                }
                Ok(HashSet::new())
            }
        };

        let changed_coins = match result {
            Ok(coins) => coins,
            Err(err) => {
                // Per-event errors (malformed Px/Sz, unrecognized diff variant) are
                // recoverable: skip the offending batch and keep serving every other
                // coin's state. Discarding `order_book_state` here used to take down
                // the entire feed for ~10s on a single malformed line.
                PARSE_ERRORS_TOTAL.with_label_values(&[source_label]).inc();
                log::warn!(
                    "Skipping event batch at height={} source={} due to apply error: {err}",
                    height,
                    source_label
                );
                HashSet::new()
            }
        };
        EVENT_PROCESSING_LATENCY.with_label_values(&[source_label]).observe(process_start.elapsed().as_secs_f64());

        // Log HFT state progress periodically
        static HFT_STATE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sc = HFT_STATE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sc % 1000 == 0 {
            if let Some(state) = &mut self.order_book_state {
                // Record health metrics
                ORDERBOOK_HEIGHT.set(state.height() as i64);
                ORDERBOOK_TIME_MS.set(state.time() as i64);
                PENDING_ORDERS_CACHE.set(state.pending_order_statuses_count() as i64);
                PENDING_DIFFS_CACHE.set(state.pending_new_diffs_count() as i64);

                // Record orderbook stats
                ORDERBOOK_ORDERS_TOTAL.set(state.order_count() as i64);
                ORDERBOOK_COINS_COUNT.set(state.coin_count() as i64);

                // Cleanup stale pending entries to prevent unbounded memory growth
                state.cleanup_stale_pending();

                info!(
                    "State progress #{}: height={}, pending_statuses={}, pending_diffs={}",
                    sc,
                    state.height(),
                    state.pending_order_statuses_count(),
                    state.pending_new_diffs_count()
                );
            }
        }

        // Fast BBO broadcast - ONLY for coins that changed AND only when someone is
        // listening. Without the receiver-count gate we'd `get_bbos_for_coins` and
        // spawn a tokio task per change even with zero subscribers, wasting CPU.
        if !changed_coins.is_empty() {
            if self.features.l2book() {
                self.pending_l2_changed_coins.extend(changed_coins.iter().cloned());
            }
            if self.features.allbbo()
                && self.allbbo_subscription_registry.has_active()
                && self.internal_message_tx.as_ref().is_some_and(|tx| tx.receiver_count() > 0)
            {
                self.pending_allbbo_changed_coins.extend(changed_coins.iter().cloned());
            }
            self.broadcast_universe_if_changed(&changed_coins);

            if self.features.bbo() {
                if let Some(state) = &self.order_book_state {
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let bbo_start = Instant::now();
                            let (time, bbos) = state.get_bbos_for_coins(&changed_coins);
                            static BBO_BROADCAST_COUNT: std::sync::atomic::AtomicU64 =
                                std::sync::atomic::AtomicU64::new(0);
                            let bc = BBO_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if bc % 1000 == 0 {
                                info!("Fast BBO broadcast #{} at time {} for {} coins", bc, time, changed_coins.len());
                            }
                            // broadcast::Sender::send is non-blocking; the previous
                            // tokio::spawn wrapper added task overhead with no benefit.
                            let msg = Arc::new(InternalMessage::BboUpdate { bbos, time });
                            drop(tx.send(msg));
                            BBO_BROADCAST_LATENCY.observe(bbo_start.elapsed().as_secs_f64());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Per-coin L2 snapshots, one inner map per coin holding all aggregation variants.
/// The inner maps are Arc'd so the listener-side cache and the broadcast Arc can
/// share unchanged coins' data without deep-cloning their level vectors.
pub(crate) struct L2Snapshots(HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>);

impl L2Snapshots {
    #[cfg(test)]
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>> {
        &self.0
    }

    pub(crate) fn into_prepared_books(
        self,
        active_keys: &HashSet<L2SubscriptionKey>,
        time: u64,
        prepared_hashes: &mut HashMap<L2SubscriptionKey, u64>,
        next_version: &mut u64,
    ) -> HashMap<L2SubscriptionKey, Arc<PreparedL2Book>> {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };

        prepared_hashes.retain(|key, _| active_keys.contains(key));
        let mut prepared = HashMap::new();
        for key in active_keys {
            let Some(variants) = self.0.get(&key.coin) else { continue };
            let Some(snapshot) = variants.get(&key.params) else { continue };
            let levels = snapshot.truncate(key.n_levels).export_inner_snapshot();

            let mut hasher = DefaultHasher::new();
            levels.hash(&mut hasher);
            let hash = hasher.finish();
            if prepared_hashes.get(key).copied() == Some(hash) {
                continue;
            }

            let version = *next_version;
            *next_version = (*next_version).wrapping_add(1).max(1);
            prepared_hashes.insert(key.clone(), hash);
            let payload = L2Book::from_l2_snapshot(
                key.coin.value(),
                levels,
                time,
                key.params.n_sig_figs,
                key.params.mantissa,
                Some(key.n_levels),
            );
            prepared.insert(key.clone(), Arc::new(PreparedL2Book { version, payload }));
        }
        prepared
    }
}

struct L2FlushJob {
    generation: u64,
    time: u64,
    l2_snapshots: L2Snapshots,
    active_keys: HashSet<L2SubscriptionKey>,
    tx: Sender<Arc<InternalMessage>>,
    prepared_hashes: HashMap<L2SubscriptionKey, u64>,
    next_version: u64,
    started_at: Instant,
}

impl L2FlushJob {
    fn prepare(self) -> PreparedL2Flush {
        let prepare_start = Instant::now();
        let Self { generation, time, l2_snapshots, active_keys, tx, mut prepared_hashes, mut next_version, started_at } =
            self;
        let l2_books = l2_snapshots.into_prepared_books(&active_keys, time, &mut prepared_hashes, &mut next_version);
        L2_FLUSH_PHASE_LATENCY.with_label_values(&["prepare"]).observe(prepare_start.elapsed().as_secs_f64());
        PreparedL2Flush { generation, tx, l2_books, prepared_hashes, next_version, started_at }
    }
}

struct PreparedL2Flush {
    generation: u64,
    tx: Sender<Arc<InternalMessage>>,
    l2_books: HashMap<L2SubscriptionKey, Arc<PreparedL2Book>>,
    prepared_hashes: HashMap<L2SubscriptionKey, u64>,
    next_version: u64,
    started_at: Instant,
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

#[derive(Default)]
struct StatsAccumulator {
    fills: u64,
    ops: u64,
    blocks: HashSet<u64>,
    height: u64,
}

impl StatsAccumulator {
    fn record(&mut self, event_batch: &EventBatch, height: u64) {
        match event_batch {
            EventBatch::Fills(batch) => {
                let fills = batch.events_len() as u64;
                self.fills = self.fills.saturating_add(fills);
                self.ops = self.ops.saturating_add(fills);
            }
            EventBatch::BookDiffs(batch) => {
                let ops = batch
                    .events_ref()
                    .iter()
                    .map(|diff| match &diff.raw_book_diff {
                        crate::types::OrderDiff::New { .. } => 1,
                        crate::types::OrderDiff::Update { .. } => 1,
                        crate::types::OrderDiff::Remove => 1,
                    })
                    .sum::<u64>();
                self.ops = self.ops.saturating_add(ops);
            }
            EventBatch::Orders(_) => {}
        }
        self.blocks.insert(height);
        self.height = self.height.max(height);
    }

    fn flush(&mut self, time: u64) -> Stats {
        let stats = Stats { time, tps: self.fills, bps: self.blocks.len() as u64, height: self.height, ops: self.ops };
        self.fills = 0;
        self.ops = 0;
        self.blocks.clear();
        stats
    }
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    L2Update {
        l2_books: HashMap<L2SubscriptionKey, Arc<PreparedL2Book>>,
    },
    Universe {
        universe: HashSet<Coin>,
    },
    Fills {
        batch: Batch<NodeDataFill>,
    },
    /// Fast BBO broadcast path - bypasses expensive L2 snapshot computation
    BboUpdate {
        bbos: HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
        time: u64,
    },
    AllBboUpdate {
        bbos: Vec<(Coin, RawBbo)>,
        time: u64,
    },
    /// HFT L4 streaming - order diffs without waiting for status pairing
    L4OrderDiffs {
        batch: Batch<NodeDataOrderDiff>,
    },
    /// HFT L4 streaming - order statuses without waiting for diff pairing
    L4OrderStatuses {
        batch: Batch<NodeDataOrderStatus>,
    },
    Stats {
        stats: Stats,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct L2SubscriptionKey {
    coin: Coin,
    params: L2SnapshotParams,
    n_levels: usize,
}

impl L2SubscriptionKey {
    pub(crate) fn new(coin: Coin, n_sig_figs: Option<u32>, mantissa: Option<u64>, n_levels: Option<usize>) -> Self {
        Self { coin, params: L2SnapshotParams::new(n_sig_figs, mantissa), n_levels: n_levels.unwrap_or(DEFAULT_LEVELS) }
    }
}

pub(crate) struct PreparedL2Book {
    version: u64,
    payload: L2Book,
}

impl PreparedL2Book {
    pub(crate) const fn version(&self) -> u64 {
        self.version
    }

    pub(crate) fn payload(&self) -> &L2Book {
        &self.payload
    }
}

// ============================================================================
// HFT-OPTIMIZED VERSION
// Uses parallel file watchers and immediate OrderDiff processing
// ============================================================================

/// HFT-optimized listener using parallel file watchers
/// Key differences from hl_listen:
/// 1. 3 dedicated threads for file watching (parallel I/O)
/// 2. Processes OrderDiffs immediately (doesn't wait for OrderStatuses)
/// 3. Uses process time instead of block time for lowest latency
pub(crate) async fn hl_listen_hft(listener: Arc<Mutex<OrderBookListener>>, config: crate::ServerConfig) -> Result<()> {
    let dir = match config.data_dir.clone() {
        Some(d) => d,
        None => dirs::home_dir().ok_or(
            "Could not resolve a data directory: pass --data-dir explicitly. The default \
             requires a usable HOME environment variable, which was not set or is invalid.",
        )?,
    };

    info!("Starting HFT-optimized listener");
    info!("Data directory: {:?}", dir);
    info!("Enabled features: {}", config.features);

    // Create SnapshotConfig from ServerConfig
    let snapshot_config = SnapshotConfig {
        mode: config.snapshot_mode,
        docker_container: config.docker_container.clone(),
        hlnode_binary: config.hlnode_binary.clone(),
        abci_state_path: config.abci_state_path.clone(),
        snapshot_output_path: config.snapshot_output_path.clone(),
        visor_state_path: config.visor_state_path.clone(),
        data_dir: dir.clone(),
    };

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // Start only the file watchers needed by the enabled features.
    let (crossbeam_rx, _handles, _last_os, _last_fills, _last_diffs) =
        parallel::start_parallel_file_watchers(dir, config.features);

    // Bridge crossbeam to tokio mpsc.
    // BOUNDED channel: under processing stalls (mutex contention, slow L2 compute),
    // an unbounded queue accumulates multi-KB JSON strings indefinitely - a primary
    // OOM vector. A bounded channel applies backpressure into the bridge thread,
    // which in turn lets the crossbeam buffer absorb the burst.
    let (tokio_tx, mut tokio_rx) = tokio::sync::mpsc::channel::<parallel::FileEvent>(10_000);

    // Spawn a blocking task to bridge crossbeam -> tokio
    tokio::task::spawn_blocking(move || {
        info!("Bridge task started");
        let mut event_count = 0u64;
        loop {
            match crossbeam_rx.recv() {
                Ok(event) => {
                    event_count += 1;
                    if event_count % 100_000 == 0 {
                        info!("Bridge: received {} events", event_count);
                    }
                    if tokio_tx.blocking_send(event).is_err() {
                        error!("Bridge: tokio channel closed");
                        break;
                    }
                }
                Err(_) => {
                    error!("Bridge: crossbeam channel closed");
                    break;
                }
            }
        }
    });

    // Snapshot fetch channel
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<SnapshotTaskResult>();
    let refresh_interval = snapshot_refresh_interval(config.snapshot_refresh_hours);
    if let Some(interval) = refresh_interval {
        info!("Snapshot refresh enabled every {} seconds", interval.as_secs());
    } else {
        info!("Snapshot refresh disabled after startup snapshot");
    }

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = tokio::time::interval_at(start, Duration::from_secs(10));
    let mut l2_flush_ticker = tokio::time::interval_at(
        Instant::now() + Duration::from_millis(L2_FLUSH_TICK_MS),
        Duration::from_millis(L2_FLUSH_TICK_MS),
    );
    l2_flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stats_ticker = tokio::time::interval_at(Instant::now() + Duration::from_secs(1), Duration::from_secs(1));
    stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut snapshot_fetch_pending = false;
    let mut next_refresh_at: Option<Instant> = None;

    info!("Main event loop starting");

    loop {
        tokio::select! {
            biased;

            // Flush throttled book-derived batches on the scheduler, ahead of file
            // events in this biased select, so a full file-event queue cannot starve them.
            _ = l2_flush_ticker.tick() => {
                if config.features.allbbo() {
                    listener.lock().await.flush_allbbo_if_due();
                }
                if config.features.l2book() {
                    let job = listener.lock().await.take_l2_flush_job_if_due();
                    if let Some(job) = job {
                        let prepared = job.prepare();
                        listener.lock().await.finish_l2_flush(prepared);
                    }
                }
            }

            _ = stats_ticker.tick(), if config.features.stats() => {
                listener.lock().await.flush_stats_if_due();
            }

            // Handle completed snapshots ahead of a continuously ready file-event
            // queue. In a biased select, placing this after file events can delay the
            // result until a large upstream backlog has been fully drained.
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                snapshot_fetch_pending = false;
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(SnapshotTaskResult { kind: SnapshotTaskKind::Initial, result: Err(err), .. }) => {
                        warn!("Initial snapshot failed; retrying on the next scheduler tick: {err}");
                    }
                    Some(SnapshotTaskResult { kind: SnapshotTaskKind::Refresh, result: Err(err), .. }) => {
                        warn!("Background snapshot refresh failed; keeping current order book: {err}");
                        next_refresh_at = refresh_interval.map(|interval| Instant::now() + interval);
                    }
                    Some(SnapshotTaskResult { kind, result: Ok(outcome) }) => {
                        match outcome {
                            SnapshotInstallOutcome::Installed { replayed_lines } => {
                                info!("{kind:?} snapshot installed after replaying {replayed_lines} cached lines");
                            }
                            SnapshotInstallOutcome::SkippedReplayOverflow { cached_lines, cached_bytes } => {
                                warn!(
                                    "{kind:?} snapshot was not installed because the replay journal failed \
                                     ({cached_lines} lines / {cached_bytes} bytes)"
                                );
                            }
                            SnapshotInstallOutcome::SkippedStaleSnapshot {
                                snapshot_height,
                                replay_cutoff_height,
                                started_book_height,
                            } => {
                                warn!(
                                    "{kind:?} snapshot was not installed because replay cutoff {replay_cutoff_height} \
                                     for snapshot height {snapshot_height} was behind the served book height \
                                     {started_book_height} at replay start"
                                );
                            }
                        }
                        next_refresh_at = refresh_interval.map(|interval| Instant::now() + interval);
                    }
                }
            }

            // Process events from file watchers (via bridge)
            Some(event) = tokio_rx.recv() => {
                match event {
                    parallel::FileEvent::OrderDiff(line) => {
                        // Process OrderDiff immediately - this is the BBO-critical path
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::OrderDiffs) {
                            error!("OrderDiff error: {err}");
                        }
                    }
                    parallel::FileEvent::OrderStatus(line) => {
                        // OrderStatuses are less latency-critical
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::OrderStatuses) {
                            error!("OrderStatus error: {err}");
                        }
                    }
                    parallel::FileEvent::Fill(line) => {
                        // Fills are for trade data, not BBO
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::Fills) {
                            error!("Fill error: {err}");
                        }
                    }
                }
            }

            // Periodic snapshot fetch: startup snapshot until ready, then recurring refreshes.
            _ = ticker.tick() => {
                let now = Instant::now();
                let is_ready = listener.lock().await.is_ready();
                if config.features.requires_book_state() && is_ready && next_refresh_at.is_none() {
                    next_refresh_at = refresh_interval.map(|interval| now + interval);
                }
                info!(
                    "Ticker: is_ready={}, snapshot_fetch_pending={}, next_refresh_s={:?}",
                    is_ready,
                    snapshot_fetch_pending,
                    next_refresh_at.map(|refresh_at| refresh_at.saturating_duration_since(now).as_secs())
                );
                if let Some(kind) = next_snapshot_trigger(
                    config.features,
                    is_ready,
                    snapshot_fetch_pending,
                    next_refresh_at,
                    now,
                ) {
                    snapshot_fetch_pending = true;
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    fetch_snapshot(kind, snapshot_config.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use tokio::sync::broadcast::{Receiver, Sender, channel};

    use super::*;
    use crate::{
        order_book::{OrderBook, Side, multi_book::Snapshots},
        types::inner::InnerL4Order,
    };

    #[test]
    fn stats_accumulator_counts_ops_fills_unique_blocks_and_preserves_height() {
        let mut stats = StatsAccumulator::default();
        let fills: Batch<NodeDataFill> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 100,
            "events": [
                [
                    "0x0000000000000000000000000000000000000001",
                    {
                        "coin": "BTC",
                        "px": "50000.0",
                        "sz": "0.1",
                        "side": "A",
                        "time": 1700000000000u64,
                        "startPosition": "0",
                        "dir": "Open Long",
                        "closedPnl": "0",
                        "hash": "0xabc",
                        "oid": 123u64,
                        "crossed": true,
                        "fee": "0.5",
                        "tid": 999u64,
                        "feeToken": "USDC",
                        "liquidation": null
                    }
                ],
                [
                    "0x0000000000000000000000000000000000000002",
                    {
                        "coin": "ETH",
                        "px": "3000.0",
                        "sz": "1.0",
                        "side": "B",
                        "time": 1700000000001u64,
                        "startPosition": "0",
                        "dir": "Open Short",
                        "closedPnl": "0",
                        "hash": "0xdef",
                        "oid": 456u64,
                        "crossed": true,
                        "fee": "0.5",
                        "tid": 1000u64,
                        "feeToken": "USDC",
                        "liquidation": null
                    }
                ]
            ]
        }))
        .expect("fills parse");
        let diffs: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 101,
            "events": [
                {
                    "user": "0x0000000000000000000000000000000000000000",
                    "oid": 1u64,
                    "px": "100",
                    "coin": "BTC",
                    "raw_book_diff": { "new": { "sz": "1" } }
                },
                {
                    "user": "0x0000000000000000000000000000000000000000",
                    "oid": 2u64,
                    "px": "100",
                    "coin": "BTC",
                    "raw_book_diff": { "update": { "origSz": "1", "newSz": "2" } }
                },
                {
                    "user": "0x0000000000000000000000000000000000000000",
                    "oid": 3u64,
                    "px": "100",
                    "coin": "BTC",
                    "raw_book_diff": "remove"
                }
            ]
        }))
        .expect("diffs parse");

        stats.record(&EventBatch::Fills(fills), 100);
        stats.record(&EventBatch::BookDiffs(diffs), 101);

        let first = stats.flush(1_750_000_000_000);
        assert_eq!(first.time, 1_750_000_000_000);
        assert_eq!(first.tps, 2);
        assert_eq!(first.ops, 5);
        assert_eq!(first.bps, 2);
        assert_eq!(first.height, 101);

        let second = stats.flush(1_750_000_001_000);
        assert_eq!(second.tps, 0);
        assert_eq!(second.ops, 0);
        assert_eq!(second.bps, 0);
        assert_eq!(second.height, 101);
    }

    fn inner_order(oid: u64, coin: &str, side: Side, px: &str, sz: &str) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(coin),
            side,
            limit_px: Px::parse_from_str(px).expect("valid px"),
            sz: Sz::parse_from_str(sz).expect("valid sz"),
            oid,
            timestamp: 0,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: "0".to_string(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None,
        }
    }

    fn listener_with_btc_bid(tx: Sender<Arc<InternalMessage>>) -> OrderBookListener {
        listener_with_btc_bids(tx, &[(1, "100", "1")])
    }

    fn listener_with_btc_bid_for_features(tx: Sender<Arc<InternalMessage>>, features: FeatureSet) -> OrderBookListener {
        listener_with_btc_bids_for_features(tx, &[(1, "100", "1")], features)
    }

    fn listener_with_btc_bids(tx: Sender<Arc<InternalMessage>>, bids: &[(u64, &str, &str)]) -> OrderBookListener {
        listener_with_coin_bids(tx, &[("BTC", bids)])
    }

    fn listener_with_btc_bids_for_features(
        tx: Sender<Arc<InternalMessage>>,
        bids: &[(u64, &str, &str)],
        features: FeatureSet,
    ) -> OrderBookListener {
        listener_with_coin_bids_for_features(tx, &[("BTC", bids)], features)
    }

    fn listener_with_coin_bids(
        tx: Sender<Arc<InternalMessage>>,
        books: &[(&str, &[(u64, &str, &str)])],
    ) -> OrderBookListener {
        listener_with_coin_bids_for_features(tx, books, FeatureSet::all())
    }

    fn listener_with_coin_bids_for_features(
        tx: Sender<Arc<InternalMessage>>,
        books: &[(&str, &[(u64, &str, &str)])],
        features: FeatureSet,
    ) -> OrderBookListener {
        let snapshots = snapshots_from_coin_bids(books);
        let mut listener = OrderBookListener::new(Some(tx), false, features);
        listener.init_from_snapshot(snapshots, 0);
        if !features.l2book() {
            return listener;
        }

        listener.l2_subscription_registry.register_l2(default_l2_key("BTC"));
        // Seed the L2 cache with the original sizes. The regression below verifies
        // that a throttled change invalidates this cached entry before the next L2 send.
        let state = listener.order_book_state.as_ref().expect("state initialized");
        let empty = HashSet::new();
        let requested_params = listener.l2_subscription_registry.active_params();
        let (_time, seeded) =
            state.l2_snapshots_incremental(&empty, &requested_params, &mut listener.l2_snapshot_cache);
        assert_eq!(l2_best_bid_sz(&seeded, "BTC"), "1");
        listener
    }

    fn snapshots_from_coin_bids(books: &[(&str, &[(u64, &str, &str)])]) -> Snapshots<InnerL4Order> {
        let mut snapshot_map = HashMap::new();
        for (coin, bids) in books {
            let mut book = OrderBook::new();
            for (oid, px, sz) in *bids {
                book.add_order(inner_order(*oid, coin, Side::Bid, px, sz));
            }
            snapshot_map.insert(Coin::new(coin), book.to_snapshot());
        }
        Snapshots::new(snapshot_map)
    }

    fn order_book_state_with_bids(books: &[(&str, &[(u64, &str, &str)])], height: u64) -> OrderBookState {
        order_book_state_with_bids_and_time(books, height, 0)
    }

    fn order_book_state_with_bids_and_time(
        books: &[(&str, &[(u64, &str, &str)])],
        height: u64,
        time: u64,
    ) -> OrderBookState {
        OrderBookState::from_snapshot(snapshots_from_coin_bids(books), height, time, true, false)
    }

    fn listener_without_snapshot(tx: Sender<Arc<InternalMessage>>, features: FeatureSet) -> OrderBookListener {
        OrderBookListener::new(Some(tx), false, features)
    }

    fn features(value: &str) -> FeatureSet {
        value.parse().expect("valid features")
    }

    fn update_diff_line(coin: &str, oid: u64, orig_sz: &str, new_sz: &str) -> String {
        update_diff_line_at_block(coin, oid, orig_sz, new_sz, 1)
    }

    fn update_diff_line_at_block(coin: &str, oid: u64, orig_sz: &str, new_sz: &str, block_number: u64) -> String {
        serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": block_number,
            "events": [{
                "user": "0x0000000000000000000000000000000000000000",
                "oid": oid,
                "px": "100",
                "coin": coin,
                "raw_book_diff": {
                    "update": {
                        "origSz": orig_sz,
                        "newSz": new_sz
                    }
                }
            }]
        })
        .to_string()
    }

    fn order_status_line(coin: &str, oid: u64, user: &str) -> String {
        serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 1,
            "events": [{
                "time": "2024-01-15T10:30:00",
                "user": user,
                "hash": "0xabc",
                "builder": null,
                "status": "open",
                "order": {
                    "user": user,
                    "coin": coin,
                    "side": "B",
                    "limitPx": "100.0",
                    "sz": "1.0",
                    "oid": oid,
                    "timestamp": 1000,
                    "triggerCondition": "N/A",
                    "isTrigger": false,
                    "triggerPx": "0.0",
                    "children": [],
                    "isPositionTpsl": false,
                    "reduceOnly": false,
                    "orderType": "Limit",
                    "origSz": "1.0",
                    "tif": "Gtc",
                    "cloid": null
                }
            }]
        })
        .to_string()
    }

    fn fill_line(coin: &str) -> String {
        serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 1,
            "events": [[
                "0x0000000000000000000000000000000000000001",
                {
                    "coin": coin,
                    "px": "50000.0",
                    "sz": "0.1",
                    "side": "A",
                    "time": 1700000000000u64,
                    "startPosition": "0",
                    "dir": "Open Long",
                    "closedPnl": "0",
                    "hash": "0xabc",
                    "oid": 123u64,
                    "crossed": true,
                    "fee": "0.5",
                    "tid": 999u64,
                    "feeToken": "USDC",
                    "liquidation": null
                }
            ]]
        })
        .to_string()
    }

    fn drain_all(rx: &mut Receiver<Arc<InternalMessage>>) -> Vec<Arc<InternalMessage>> {
        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        messages
    }

    fn drain_latest_l2(rx: &mut Receiver<Arc<InternalMessage>>) -> Option<Arc<InternalMessage>> {
        let mut latest = None;
        while let Ok(msg) = rx.try_recv() {
            if matches!(msg.as_ref(), InternalMessage::L2Update { .. }) {
                latest = Some(msg);
            }
        }
        latest
    }

    fn drain_latest_allbbo(rx: &mut Receiver<Arc<InternalMessage>>) -> Option<Arc<InternalMessage>> {
        let mut latest = None;
        while let Ok(msg) = rx.try_recv() {
            if matches!(msg.as_ref(), InternalMessage::AllBboUpdate { .. }) {
                latest = Some(msg);
            }
        }
        latest
    }

    fn l2_best_bid_sz(snapshots: &L2Snapshots, coin: &str) -> String {
        l2_bid_sz_at_level(snapshots, coin, 0)
    }

    fn l2_bid_sz_at_level(snapshots: &L2Snapshots, coin: &str, level_index: usize) -> String {
        let coin_snapshots = snapshots.as_ref().get(&Coin::new(coin)).expect("coin snapshot");
        let snapshot = coin_snapshots.get(&L2SnapshotParams::new(None, None)).expect("default l2 snapshot").clone();
        let levels = snapshot.truncate(level_index + 1).export_inner_snapshot();
        levels[0].get(level_index).expect("bid level").sz().to_string()
    }

    fn default_l2_key(coin: &str) -> L2SubscriptionKey {
        L2SubscriptionKey::new(Coin::new(coin), None, None, None)
    }

    fn prepared_l2_bid_sz_at_level(
        l2_books: &HashMap<L2SubscriptionKey, Arc<PreparedL2Book>>,
        coin: &str,
        level_index: usize,
    ) -> String {
        let prepared = l2_books.get(&default_l2_key(coin)).expect("prepared l2 book");
        prepared.payload().levels()[0].get(level_index).expect("bid level").sz().to_string()
    }

    fn current_bid_sz(listener: &OrderBookListener, coin: &str) -> String {
        let state = listener.order_book_state.as_ref().expect("state initialized");
        let coins = HashSet::from([Coin::new(coin)]);
        let (_time, bbos) = state.get_bbos_for_coins(&coins);
        bbos.get(&Coin::new(coin)).expect("coin bbo").0.as_ref().expect("bid").1.to_str()
    }

    #[test]
    fn snapshot_refresh_interval_treats_zero_as_disabled() {
        assert_eq!(snapshot_refresh_interval(0), None);
        assert_eq!(snapshot_refresh_interval(4).expect("enabled").as_secs(), 14_400);
    }

    #[test]
    fn snapshot_scheduler_triggers_initial_and_refresh_only_when_due() {
        let now = Instant::now();
        assert_eq!(next_snapshot_trigger(features("bbo"), false, false, None, now), Some(SnapshotTaskKind::Initial));
        assert_eq!(
            next_snapshot_trigger(features("bbo"), true, false, Some(now), now),
            Some(SnapshotTaskKind::Refresh)
        );
        assert_eq!(next_snapshot_trigger(features("bbo"), true, false, None, now), None);
        assert_eq!(next_snapshot_trigger(features("bbo"), true, false, Some(now + Duration::from_secs(1)), now), None);
        assert_eq!(next_snapshot_trigger(features("bbo"), false, true, None, now), None);
        assert_eq!(next_snapshot_trigger(features("trades"), true, false, Some(now), now), None);
    }

    #[test]
    fn initial_snapshot_replay_uses_larger_startup_budget() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));

        listener.begin_snapshot_replay();
        let refresh_limits = listener.snapshot_replay_cache.as_ref().expect("refresh cache").limits;

        listener.begin_snapshot_replay_for(SnapshotTaskKind::Initial).expect("initial replay journal should open");
        let initial_limits = listener.snapshot_replay_cache.as_ref().expect("initial cache").limits;

        assert!(initial_limits.max_lines > refresh_limits.max_lines);
        assert!(initial_limits.max_bytes > refresh_limits.max_bytes);
    }

    #[test]
    fn snapshot_replay_journal_enforces_its_disk_safety_limit() {
        let mut cache = SnapshotReplayCache::new(0, SnapshotTaskKind::Initial).expect("replay journal should open");
        cache.limits = SnapshotReplayLimits { max_lines: 1, max_bytes: usize::MAX };

        assert_eq!(cache.push(EventSource::OrderDiffs, "{}"), None);
        let failure = cache.push(EventSource::OrderDiffs, "{}").expect("second line should exceed the limit");

        assert!(cache.overflowed);
        assert_eq!(cache.line_count, 1);
        assert_eq!(cache.total_bytes, 2);
        assert!(failure.contains("exceeded its limits"));
    }

    #[test]
    fn embedded_height_snapshot_does_not_require_visor_height() {
        let (replay_cutoff_height, visor_heights) = snapshot_replay_cutoff_height(
            SnapshotHeightSource::Embedded,
            42,
            Err("missing before visor".into()),
            Err("missing after visor".into()),
        )
        .expect("embedded height does not need visor fallback");

        assert_eq!(replay_cutoff_height, 42);
        assert_eq!(visor_heights, None);
    }

    #[test]
    fn bare_snapshot_requires_visor_heights() {
        assert!(
            snapshot_replay_cutoff_height(SnapshotHeightSource::Visor, 42, Err("missing before visor".into()), Ok(43),)
                .is_err()
        );
        assert!(
            snapshot_replay_cutoff_height(SnapshotHeightSource::Visor, 42, Ok(41), Err("missing after visor".into()),)
                .is_err()
        );
    }

    #[test]
    fn bare_snapshot_uses_lower_observed_visor_height_as_replay_cutoff() {
        let (replay_cutoff_height, visor_heights) =
            snapshot_replay_cutoff_height(SnapshotHeightSource::Visor, 42, Ok(40), Ok(42))
                .expect("visor heights are available");

        assert_eq!(replay_cutoff_height, 40);
        assert_eq!(visor_heights, Some((40, 42)));
    }

    #[test]
    fn initial_snapshot_ignores_cached_and_queued_lines_at_or_below_snapshot_height() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "2", "4", 10), EventSource::OrderDiffs)
            .expect("cached diff records during startup replay");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 20);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 20, 20, SnapshotTaskKind::Initial)
            .expect("initial snapshot installs");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "2");
        drop(drain_all(&mut rx));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "4", "5", 12), EventSource::OrderDiffs)
            .expect("queued diff below snapshot height is ignored");

        assert_eq!(current_bid_sz(&listener, "BTC"), "2");
        assert!(!drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "2", "5", 21), EventSource::OrderDiffs)
            .expect("queued diff above snapshot height applies");

        assert_eq!(current_bid_sz(&listener, "BTC"), "5");
        assert!(drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn initial_snapshot_replays_cached_lines_above_snapshot_height() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "2", "4", 21), EventSource::OrderDiffs)
            .expect("cached diff records during startup replay");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 20);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 20, 20, SnapshotTaskKind::Initial)
            .expect("initial snapshot installs");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
    }

    #[test]
    fn initial_visor_snapshot_uses_lower_replay_cutoff_when_height_advances() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "2", "4", 21), EventSource::OrderDiffs)
            .expect("cached diff records during startup replay");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 22, 20, SnapshotTaskKind::Initial)
            .expect("initial visor snapshot installs with conservative replay cutoff");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
        assert!(listener.snapshot_replay_cache.is_none());
    }

    #[test]
    fn queued_line_above_visor_replay_cutoff_applies_after_swap() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 22, 20, SnapshotTaskKind::Initial)
            .expect("initial visor snapshot installs with conservative replay cutoff");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        drop(drain_all(&mut rx));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "2", "5", 21), EventSource::OrderDiffs)
            .expect("queued diff above replay cutoff applies after swap");

        assert_eq!(current_bid_sz(&listener, "BTC"), "5");
        assert!(drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn queued_line_in_visor_guard_window_does_not_roll_back_snapshot() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "5")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 22, 20, SnapshotTaskKind::Initial)
            .expect("initial visor snapshot installs with conservative replay cutoff");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        drop(drain_all(&mut rx));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 21), EventSource::OrderDiffs)
            .expect("queued diff in snapshot window is replay-guarded");

        assert_eq!(current_bid_sz(&listener, "BTC"), "5");
        assert!(!drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn queued_line_above_snapshot_height_uses_live_semantics_after_swap() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "5")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 22, 20, SnapshotTaskKind::Initial)
            .expect("initial visor snapshot installs with conservative replay cutoff");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        drop(drain_all(&mut rx));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 23), EventSource::OrderDiffs)
            .expect("queued diff above snapshot height uses live self-healing semantics");

        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
        assert!(drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn replay_update_with_mismatched_orig_size_does_not_roll_back_snapshot() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 21), EventSource::OrderDiffs)
            .expect("cached diff records during startup replay");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "5")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(new_state, 22, 20, SnapshotTaskKind::Initial)
            .expect("initial visor snapshot installs with conservative replay cutoff");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "5");
    }

    #[test]
    fn refresh_with_visor_snapshot_height_installs() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.replace_order_book_state(
            order_book_state_with_bids(&[("BTC", &[(1, "100", "3")])], 12),
            12,
            12,
            false,
        );
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 13), EventSource::OrderDiffs)
            .expect("live diff applies");
        drop(drain_all(&mut rx));

        let refreshed_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 20);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(refreshed_state, 20, 20, SnapshotTaskKind::Refresh)
            .expect("visor-height refresh installs");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "2");
        assert!(listener.snapshot_replay_cache.is_none());
        assert!(drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn refresh_visor_snapshot_uses_lower_replay_cutoff_when_height_advances() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.replace_order_book_state(
            order_book_state_with_bids(&[("BTC", &[(1, "100", "3")])], 12),
            12,
            12,
            false,
        );
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 21), EventSource::OrderDiffs)
            .expect("live diff applies");
        drop(drain_all(&mut rx));

        let refreshed_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "3")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(refreshed_state, 22, 20, SnapshotTaskKind::Refresh)
            .expect("refresh installs with conservative replay cutoff");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
        assert!(listener.snapshot_replay_cache.is_none());
        assert!(drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn refresh_visor_snapshot_with_cutoff_below_replay_start_is_skipped() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.replace_order_book_state(
            order_book_state_with_bids(&[("BTC", &[(1, "100", "3")])], 12),
            12,
            12,
            false,
        );
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 13), EventSource::OrderDiffs)
            .expect("live diff applies");
        drop(drain_all(&mut rx));

        let refreshed_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "3")])], 22);
        let outcome = listener
            .install_snapshot_state_with_replay_cutoff(refreshed_state, 22, 11, SnapshotTaskKind::Refresh)
            .expect("unsafe refresh skips without failing");

        assert_eq!(
            outcome,
            SnapshotInstallOutcome::SkippedStaleSnapshot {
                snapshot_height: 22,
                replay_cutoff_height: 11,
                started_book_height: 12,
            }
        );
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
        assert!(listener.snapshot_replay_cache.is_none());
        assert!(!drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn refresh_replay_applies_cached_lines_above_snapshot_height() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "1", "4", 11), EventSource::OrderDiffs)
            .expect("live diff applies");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "1")])], 10);
        let outcome =
            listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
    }

    #[test]
    fn refresh_replay_ignores_cached_lines_at_or_below_snapshot_height() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "1", "4", 10), EventSource::OrderDiffs)
            .expect("live diff applies");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        let outcome =
            listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "2");
    }

    #[test]
    fn refresh_replay_applies_cached_lines_above_captured_watermark() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "1", "4", 10), EventSource::OrderDiffs)
            .expect("live diff applies");

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "1")])], 9);
        let outcome = listener
            .install_snapshot_state(new_state, 9, SnapshotTaskKind::Refresh)
            .expect("refresh installs with conservative watermark");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
    }

    #[test]
    fn refresh_snapshot_below_replay_start_height_is_skipped() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.replace_order_book_state(
            order_book_state_with_bids(&[("BTC", &[(1, "100", "3")])], 12),
            12,
            12,
            false,
        );
        listener.begin_snapshot_replay();

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "3", "4", 13), EventSource::OrderDiffs)
            .expect("live diff applies");
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
        drop(drain_all(&mut rx));

        let stale_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        let outcome = listener
            .install_snapshot_state(stale_state, 10, SnapshotTaskKind::Refresh)
            .expect("stale refresh should skip without failing");

        assert_eq!(
            outcome,
            SnapshotInstallOutcome::SkippedStaleSnapshot {
                snapshot_height: 10,
                replay_cutoff_height: 10,
                started_book_height: 12,
            }
        );
        assert_eq!(current_bid_sz(&listener, "BTC"), "4");
        assert!(!drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn refresh_without_replay_preserves_current_book_time() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        listener.replace_order_book_state(
            order_book_state_with_bids_and_time(&[("BTC", &[(1, "100", "1")])], 7, 123_456),
            7,
            7,
            false,
        );
        listener.begin_snapshot_replay();

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        let outcome =
            listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");

        assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 0 });
        assert_eq!(listener.order_book_state.as_ref().expect("state initialized").time(), 123_456);
        assert!(
            drain_all(&mut rx)
                .iter()
                .any(|msg| { matches!(msg.as_ref(), InternalMessage::BboUpdate { time: 123_456, .. }) })
        );
    }

    #[test]
    fn refresh_replay_overflow_skips_refresh_but_initial_overflow_is_fatal() {
        let (tx, _rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));
        let mut overflowed_cache = SnapshotReplayCache::default();
        overflowed_cache.overflowed = true;
        listener.snapshot_replay_cache = Some(overflowed_cache);

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        let outcome =
            listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Refresh).expect("refresh overflow skips");

        assert_eq!(outcome, SnapshotInstallOutcome::SkippedReplayOverflow { cached_lines: 0, cached_bytes: 0 });
        assert_eq!(current_bid_sz(&listener, "BTC"), "1");

        let mut overflowed_cache = SnapshotReplayCache::default();
        overflowed_cache.overflowed = true;
        listener.snapshot_replay_cache = Some(overflowed_cache);
        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        assert!(listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Initial).is_err());
    }

    #[test]
    fn stream_batches_at_or_below_snapshot_height_preserve_raw_streams_after_swap() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("all"));
        listener.begin_snapshot_replay();

        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");
        drop(drain_all(&mut rx));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "1", "4", 9), EventSource::OrderDiffs)
            .expect("stale queued diff preserves raw stream but skips book state");
        assert_eq!(current_bid_sz(&listener, "BTC"), "2");
        let messages = drain_all(&mut rx);
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));

        listener
            .process_data_hft(
                order_status_line("BTC", 1, "0x0000000000000000000000000000000000000001"),
                EventSource::OrderStatuses,
            )
            .expect("stale queued status preserves raw stream but skips book state");
        let messages = drain_all(&mut rx);
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderStatuses { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));

        listener
            .process_data_hft(update_diff_line_at_block("BTC", 1, "2", "5", 11), EventSource::OrderDiffs)
            .expect("fresh diff applies");
        assert_eq!(current_bid_sz(&listener, "BTC"), "5");
        assert!(drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
    }

    #[test]
    fn refresh_swap_defers_l2_update_to_flush_without_resetting_versions() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid(tx);
        listener.allbbo_subscription_registry.register();

        listener.pending_l2_changed_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();
        let first_l2 = drain_latest_l2(&mut rx).expect("first l2 update");
        let first_version = match first_l2.as_ref() {
            InternalMessage::L2Update { l2_books } => {
                l2_books.get(&default_l2_key("BTC")).expect("prepared l2 book").version()
            }
            _ => unreachable!("drain_latest_l2 only returns L2 updates"),
        };

        listener.begin_snapshot_replay();
        let new_state = order_book_state_with_bids(&[("BTC", &[(1, "100", "2")])], 10);
        listener.install_snapshot_state(new_state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");

        let messages = drain_all(&mut rx);
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::AllBboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L2Update { .. })));
        assert!(listener.pending_l2_changed_coins.contains(&Coin::new("BTC")));

        listener.flush_l2_if_due();
        let msg = drain_latest_l2(&mut rx).expect("scheduled l2 refresh update");
        let l2_books = match msg.as_ref() {
            InternalMessage::L2Update { l2_books } => l2_books,
            _ => unreachable!("drain_latest_l2 only returns L2 updates"),
        };
        let prepared = l2_books.get(&default_l2_key("BTC")).expect("prepared l2 book");
        assert!(prepared.version() > first_version, "refresh must not reset versions for existing clients");
        assert_eq!(prepared.payload().levels()[0][0].sz().to_string(), "2");
    }

    #[test]
    fn bbo_feature_only_skips_l2_and_raw_broadcast_work() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("bbo"));

        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff applies");

        let messages = drain_all(&mut rx);
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::AllBboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L2Update { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::Fills { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderStatuses { .. })));
        assert!(listener.pending_l2_changed_coins.is_empty());
    }

    #[test]
    fn allbbo_feature_only_batches_without_per_coin_bbo() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("allbbo"));
        listener.allbbo_subscription_registry.register();
        listener.last_allbbo_broadcast = Some(Instant::now() - Duration::from_millis(ALLBBO_BROADCAST_THROTTLE_MS + 1));

        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff applies");
        assert!(listener.pending_allbbo_changed_coins.contains(&Coin::new("BTC")));

        listener.flush_allbbo_if_due();
        let messages = drain_all(&mut rx);
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::AllBboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(listener.pending_allbbo_changed_coins.is_empty());
    }

    #[test]
    fn allbbo_flush_coalesces_changed_coins_after_throttle() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_coin_bids_for_features(
            tx,
            &[("BTC", &[(1, "100", "1")]), ("ETH", &[(2, "100", "1")])],
            features("allbbo"),
        );
        listener.allbbo_subscription_registry.register();
        listener.last_allbbo_broadcast = Some(Instant::now());

        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("BTC applies");
        listener.process_data_hft(update_diff_line("ETH", 2, "1", "3"), EventSource::OrderDiffs).expect("ETH applies");

        listener.flush_allbbo_if_due();
        assert!(drain_latest_allbbo(&mut rx).is_none(), "flush should respect the 50ms throttle");
        assert!(listener.pending_allbbo_changed_coins.contains(&Coin::new("BTC")));
        assert!(listener.pending_allbbo_changed_coins.contains(&Coin::new("ETH")));

        listener.last_allbbo_broadcast = Some(Instant::now() - Duration::from_millis(ALLBBO_BROADCAST_THROTTLE_MS + 1));
        listener.flush_allbbo_if_due();
        let msg = drain_latest_allbbo(&mut rx).expect("pending allbbo changes should publish");
        match msg.as_ref() {
            InternalMessage::AllBboUpdate { bbos, .. } => {
                assert_eq!(bbos.len(), 2);
                assert!(bbos.iter().any(|(coin, _)| *coin == Coin::new("BTC")));
                assert!(bbos.iter().any(|(coin, _)| *coin == Coin::new("ETH")));
            }
            _ => unreachable!("drain_latest_allbbo only returns allbbo updates"),
        }
        assert!(listener.pending_allbbo_changed_coins.is_empty());
    }

    #[test]
    fn allbbo_skips_pending_work_without_receivers() {
        let (tx, rx) = channel::<Arc<InternalMessage>>(16);
        drop(rx);
        let mut listener = listener_with_btc_bid_for_features(tx, features("allbbo"));
        listener.allbbo_subscription_registry.register();

        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff applies");

        assert!(listener.pending_allbbo_changed_coins.is_empty());
    }

    #[test]
    fn l2book_feature_only_emits_l2_without_bbo_or_raw_streams() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid_for_features(tx, features("l2book"));

        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff applies");

        assert!(listener.pending_l2_changed_coins.contains(&Coin::new("BTC")));
        assert!(!drain_all(&mut rx).iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));

        listener.flush_l2_if_due();
        let messages = drain_all(&mut rx);
        assert!(messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L2Update { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::AllBboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderStatuses { .. })));
    }

    #[test]
    fn trades_only_is_ready_without_snapshot_and_only_broadcasts_fills() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("trades"));

        assert!(listener.is_ready());
        assert!(listener.order_book_state.is_none());

        listener.process_data_hft(fill_line("BTC"), EventSource::Fills).expect("fill parses");
        listener
            .process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs)
            .expect("disabled diff source is ignored");

        let messages = drain_all(&mut rx);
        assert_eq!(messages.iter().filter(|msg| matches!(msg.as_ref(), InternalMessage::Fills { .. })).count(), 1);
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L2Update { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. })));
    }

    #[test]
    fn bookdiffs_only_is_ready_without_snapshot_and_only_broadcasts_raw_diffs() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("bookdiffs"));

        assert!(listener.is_ready());
        assert!(listener.order_book_state.is_none());

        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff parses");
        listener.process_data_hft(fill_line("BTC"), EventSource::Fills).expect("disabled fill source is ignored");

        let messages = drain_all(&mut rx);
        assert_eq!(
            messages.iter().filter(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. })).count(),
            1
        );
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::Fills { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(listener.pending_l2_changed_coins.is_empty());
    }

    #[test]
    fn orderupdates_only_is_ready_without_snapshot_and_only_broadcasts_statuses() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_without_snapshot(tx, features("orderupdates"));
        let user = "0x0000000000000000000000000000000000000001";

        assert!(listener.is_ready());
        assert!(listener.order_book_state.is_none());

        listener
            .process_data_hft(order_status_line("BTC", 1, user), EventSource::OrderStatuses)
            .expect("status parses");
        listener
            .process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs)
            .expect("disabled diff source is ignored");

        let messages = drain_all(&mut rx);
        assert_eq!(
            messages.iter().filter(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderStatuses { .. })).count(),
            1
        );
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. })));
        assert!(!messages.iter().any(|msg| matches!(msg.as_ref(), InternalMessage::BboUpdate { .. })));
        assert!(listener.pending_l2_changed_coins.is_empty());
    }

    #[test]
    fn l2_dirty_coins_survive_throttle_window() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid(tx);

        // Suppress the immediate L2 compute for this BTC change. BBO still broadcasts
        // from live state, but L2 must remember that BTC's cached snapshot is stale.
        listener.last_l2_broadcast = Some(Instant::now());
        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff applies");

        assert!(drain_latest_l2(&mut rx).is_none(), "the BTC update should be inside the L2 throttle window");
        assert!(listener.pending_l2_changed_coins.contains(&Coin::new("BTC")));

        // The periodic L2 flush after the throttle window must recompute BTC even
        // if no additional file event arrives.
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        let msg = drain_latest_l2(&mut rx).expect("pending BTC change should force an L2 snapshot");
        match msg.as_ref() {
            InternalMessage::L2Update { l2_books, .. } => {
                assert_eq!(prepared_l2_bid_sz_at_level(l2_books, "BTC", 0), "2");
            }
            _ => unreachable!("drain_latest_l2 only returns snapshots"),
        }
        assert!(listener.pending_l2_changed_coins.is_empty());
    }

    #[test]
    fn l2_dirty_coins_recompute_non_top_levels_after_throttle() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bids(tx, &[(1, "100", "1"), (2, "99", "1")]);

        // Change only the second bid level while the top bid remains identical.
        // This proves the pending dirty set invalidates the whole coin snapshot,
        // not just the BBO-visible level that originally exposed the bug.
        listener.last_l2_broadcast = Some(Instant::now());
        listener.process_data_hft(update_diff_line("BTC", 2, "1", "2"), EventSource::OrderDiffs).expect("diff applies");

        assert!(drain_latest_l2(&mut rx).is_none(), "the second-level change should be inside the L2 throttle window");
        assert!(listener.pending_l2_changed_coins.contains(&Coin::new("BTC")));

        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        let msg = drain_latest_l2(&mut rx).expect("pending BTC change should force an L2 snapshot");
        match msg.as_ref() {
            InternalMessage::L2Update { l2_books, .. } => {
                assert_eq!(prepared_l2_bid_sz_at_level(l2_books, "BTC", 0), "1");
                assert_eq!(prepared_l2_bid_sz_at_level(l2_books, "BTC", 1), "2");
            }
            _ => unreachable!("drain_latest_l2 only returns snapshots"),
        }
        assert!(listener.pending_l2_changed_coins.is_empty());
    }

    #[test]
    fn process_data_hft_defers_l2_flush_to_scheduler() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid(tx);

        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.process_data_hft(update_diff_line("BTC", 1, "1", "2"), EventSource::OrderDiffs).expect("diff applies");

        assert!(listener.pending_l2_changed_coins.contains(&Coin::new("BTC")));
        assert!(drain_latest_l2(&mut rx).is_none(), "event processing should not synchronously flush L2");

        listener.flush_l2_if_due();

        let msg = drain_latest_l2(&mut rx).expect("scheduler flush should publish pending L2");
        match msg.as_ref() {
            InternalMessage::L2Update { l2_books, .. } => {
                assert_eq!(prepared_l2_bid_sz_at_level(l2_books, "BTC", 0), "2");
            }
            _ => unreachable!("drain_latest_l2 only returns L2 updates"),
        }
    }

    #[test]
    fn l2_update_prepares_each_active_subscription_shape() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bids(tx, &[(1, "100", "1"), (2, "99", "2")]);
        let one_level_key = L2SubscriptionKey::new(Coin::new("BTC"), None, None, Some(1));
        listener.l2_subscription_registry.register_l2(one_level_key.clone());

        listener.pending_l2_changed_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        let msg = drain_latest_l2(&mut rx).expect("pending BTC change should force an L2 update");
        match msg.as_ref() {
            InternalMessage::L2Update { l2_books, .. } => {
                assert_eq!(l2_books.len(), 2);
                assert!(l2_books.contains_key(&default_l2_key("BTC")));
                assert!(l2_books.contains_key(&one_level_key));
                assert!(l2_books.get(&default_l2_key("BTC")).expect("default book").version() > 0);
                assert_eq!(prepared_l2_bid_sz_at_level(l2_books, "BTC", 1), "2");
                assert_eq!(l2_books.get(&one_level_key).expect("one-level book").payload().levels()[0].len(), 1);
            }
            _ => unreachable!("drain_latest_l2 only returns L2 updates"),
        }
    }

    #[test]
    fn l2_update_skips_unchanged_prepared_subscription_shapes() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid(tx);

        listener.pending_l2_changed_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        let msg = drain_latest_l2(&mut rx).expect("first dirty flush should publish the prepared book");
        let first_version = match msg.as_ref() {
            InternalMessage::L2Update { l2_books, .. } => {
                l2_books.get(&default_l2_key("BTC")).expect("prepared l2 book").version()
            }
            _ => unreachable!("drain_latest_l2 only returns L2 updates"),
        };
        assert!(first_version > 0);

        listener.pending_l2_changed_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        assert!(drain_latest_l2(&mut rx).is_none(), "unchanged prepared book should be filtered before broadcast");
    }

    #[test]
    fn stale_l2_flush_job_after_snapshot_reset_is_dropped() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_btc_bid(tx);

        listener.pending_l2_changed_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        let job = listener.take_l2_flush_job_if_due().expect("flush job");
        let prepared = job.prepare();

        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 1);
        listener.finish_l2_flush(prepared);

        assert!(drain_latest_l2(&mut rx).is_none(), "stale prepared L2 should not publish after a snapshot reset");
    }

    #[test]
    fn l2_flush_skips_dirty_coins_without_active_l2_subscriptions() {
        let (tx, mut rx) = channel::<Arc<InternalMessage>>(16);
        let mut listener = listener_with_coin_bids(tx, &[("BTC", &[(1, "100", "1")]), ("ETH", &[(2, "100", "1")])]);
        let eth_before = Arc::clone(listener.l2_snapshot_cache.get(&Coin::new("ETH")).expect("ETH cache"));

        listener.pending_l2_changed_coins.insert(Coin::new("ETH"));
        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        assert!(listener.pending_l2_changed_coins.is_empty());
        assert!(Arc::ptr_eq(&eth_before, listener.l2_snapshot_cache.get(&Coin::new("ETH")).expect("ETH cache")));
        assert!(drain_latest_l2(&mut rx).is_none());
    }

    #[test]
    fn l2_flush_without_active_l2_subscribers_skips_snapshot_work() {
        let (tx, mut rx) = channel(8);
        let mut listener = listener_with_btc_bid(tx);
        listener.l2_subscription_registry.unregister_l2(&default_l2_key("BTC"));
        listener.pending_l2_changed_coins.insert(Coin::new("BTC"));
        assert!(!listener.l2_snapshot_cache.is_empty(), "test setup should seed cache");

        listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
        listener.flush_l2_if_due();

        assert!(listener.pending_l2_changed_coins.is_empty());
        assert!(listener.l2_snapshot_cache.is_empty());
        assert!(drain_latest_l2(&mut rx).is_none());
    }
}
