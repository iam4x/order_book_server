use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard},
    time::Duration,
};

use alloy::primitives::Address;
use log::{error, info};
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
    listeners::order_book::state::OrderBookState,
    metrics::{
        BBO_BROADCAST_LATENCY, EVENT_PROCESSING_LATENCY, EVENTS_PROCESSED_TOTAL, FILE_EVENTS_TOTAL,
        FILE_LINES_PARSED_TOTAL, L2_BROADCAST_LATENCY, ORDERBOOK_COINS_COUNT, ORDERBOOK_HEIGHT, ORDERBOOK_ORDERS_TOTAL,
        ORDERBOOK_TIME_MS, PARSE_ERRORS_TOTAL, PENDING_DIFFS_CACHE, PENDING_ORDERS_CACHE,
    },
    order_book::{
        Coin, Px, Snapshot, Sz,
        multi_book::{Snapshots, load_snapshots_from_cli_json},
    },
    prelude::*,
    types::{
        L2Book, L4Order,
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

    pub(crate) fn active_params(&self) -> HashSet<L2SnapshotParams> {
        self.params().keys().copied().collect()
    }

    pub(crate) fn active_coins(&self) -> HashSet<Coin> {
        self.coins().keys().cloned().collect()
    }

    pub(crate) fn active_keys(&self) -> HashSet<L2SubscriptionKey> {
        self.keys().keys().cloned().collect()
    }
}

fn fetch_snapshot(
    snapshot_config: SnapshotConfig,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    _ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        // CRITICAL: Start caching BEFORE generating snapshot
        // This ensures we don't miss any events during snapshot generation.
        // We don't clone the existing state here - it's discarded by init_from_snapshot
        // below, and cloning the whole BTreeMap/Slab tree temporarily doubles peak RSS.
        {
            let mut listener = listener.lock().await;
            listener.begin_caching();
        }

        // Now generate snapshot - any events during this time are cached
        let visor_path = get_visor_path(&snapshot_config);
        let res = match process_rmp_file(&snapshot_config).await {
            Ok(output_fln) => {
                let snapshot =
                    load_snapshots_from_cli_json::<InnerL4Order, (Address, L4Order)>(&output_fln, &visor_path).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let _cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        info!("Snapshot loaded at height {}", height);
                        // Always reinitialize from snapshot to get fresh, accurate orderbook
                        // This corrects any drift from missed streaming updates
                        listener.lock().await.init_from_snapshot(expected_snapshot, height);
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok::<(), Error>(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
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
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    last_universe: HashSet<Coin>,
}

impl OrderBookListener {
    pub(crate) fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            order_book_state: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            last_l2_broadcast: None,
            pending_l2_changed_coins: HashSet::new(),
            l2_snapshot_cache: HashMap::new(),
            l2_prepared_hashes: HashMap::new(),
            next_l2_version: 1,
            l2_subscription_registry: Arc::new(L2SubscriptionRegistry::default()),
            last_universe: HashSet::new(),
        }
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    pub(crate) fn l2_subscription_registry(&self) -> Arc<L2SubscriptionRegistry> {
        Arc::clone(&self.l2_subscription_registry)
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // take the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("Initializing from snapshot at height {}", height);
        // On initial startup, just trust the snapshot and start fresh
        // Don't try to apply cached updates - they may have gaps
        let new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        self.order_book_state = Some(new_order_book);
        self.last_universe = self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe);
        // The incremental L2 cache references the previous book's coins/levels;
        // drop it so the next broadcast does a full rebuild against the new state.
        self.l2_snapshot_cache = HashMap::new();
        self.l2_prepared_hashes.clear();
        self.next_l2_version = 1;
        self.pending_l2_changed_coins.clear();
        // Clear any stale cache
        self.fetched_snapshot_cache = None;
        info!("Order book ready at height {}", height);
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
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

    fn flush_l2_if_due(&mut self) {
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
        let requested_params = self.l2_subscription_registry.active_params();
        let active_coins = self.l2_subscription_registry.active_coins();
        let active_keys = self.l2_subscription_registry.active_keys();
        if requested_params.is_empty() || active_coins.is_empty() || active_keys.is_empty() {
            self.pending_l2_changed_coins.clear();
            self.l2_snapshot_cache.clear();
            self.l2_prepared_hashes.clear();
            return;
        }
        if !self.pending_l2_changed_coins.iter().any(|coin| active_coins.contains(coin)) {
            self.pending_l2_changed_coins.clear();
            return;
        }

        let should_broadcast_l2 = !self.pending_l2_changed_coins.is_empty()
            && self
                .last_l2_broadcast
                .map(|t| t.elapsed() >= Duration::from_millis(L2_BROADCAST_THROTTLE_MS))
                .unwrap_or(true);

        if should_broadcast_l2 {
            let has_receivers = self.internal_message_tx.as_ref().is_some_and(|tx| tx.receiver_count() > 0);
            // Mark the throttle as fired regardless of receivers so we don't
            // re-check on every subsequent event when nobody is listening.
            self.last_l2_broadcast = Some(Instant::now());
            if has_receivers {
                if let Some(state) = &self.order_book_state {
                    let l2_start = Instant::now();
                    let changed_for_l2: HashSet<Coin> = std::mem::take(&mut self.pending_l2_changed_coins)
                        .into_iter()
                        .filter(|coin| active_coins.contains(coin))
                        .collect();
                    let (time, l2_snapshots) =
                        state.l2_snapshots_incremental(&changed_for_l2, &requested_params, &mut self.l2_snapshot_cache);

                    static L2_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                    let bc = L2_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if bc % 100 == 0 {
                        info!("L2 broadcast #{} at time {}", bc, time);
                    }

                    if let Some(tx) = &self.internal_message_tx {
                        let l2_books = l2_snapshots.into_prepared_books(
                            &active_keys,
                            time,
                            &mut self.l2_prepared_hashes,
                            &mut self.next_l2_version,
                        );
                        if !l2_books.is_empty() {
                            let msg = Arc::new(InternalMessage::L2Update { l2_books });
                            drop(tx.send(msg));
                        }
                    }
                    L2_BROADCAST_LATENCY.observe(l2_start.elapsed().as_secs_f64());
                }
            }
        }
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
        let changed_coins: HashSet<Coin> = if let Some(state) = self.order_book_state.as_mut() {
            let result = match event_batch {
                EventBatch::Orders(batch) => {
                    // Broadcast L4 order statuses for L4Book subscribers - skip the
                    // batch clone entirely when nothing is subscribed (the per-conn
                    // filter inside handle_socket already short-circuits, but the
                    // clone is what costs us in OOM scenarios).
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let msg = Arc::new(InternalMessage::L4OrderStatuses { batch: batch.clone() });
                            drop(tx.send(msg));
                        }
                    }
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["orders"]).inc();
                    state.apply_order_statuses_hft(batch)
                }
                EventBatch::BookDiffs(batch) => {
                    // Broadcast L4 order diffs for L4Book / BookDiffs subscribers.
                    // Defense-in-depth: when running with `ignore_spot=true`, strip
                    // spot diffs from the broadcast too. Otherwise `bookDiffs` clients
                    // would see events for coins whose state we never applied locally.
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let to_broadcast = if state.ignore_spot() {
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
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["diffs"]).inc();
                    state.apply_order_diffs_hft(batch)
                }
                EventBatch::Fills(batch) => {
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["fills"]).inc();
                    // Broadcast fills (no clone needed - we own the batch and don't apply it locally)
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            drop(tx.send(snapshot));
                        }
                    }
                    Ok(HashSet::new())
                }
            };

            match result {
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
            }
        } else {
            HashSet::new()
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
            self.pending_l2_changed_coins.extend(changed_coins.iter().cloned());
            self.broadcast_universe_if_changed(&changed_coins);

            if let Some(state) = &self.order_book_state {
                if let Some(tx) = &self.internal_message_tx {
                    if tx.receiver_count() > 0 {
                        let bbo_start = Instant::now();
                        let (time, bbos) = state.get_bbos_for_coins(&changed_coins);
                        static BBO_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
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

        self.flush_l2_if_due();
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

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
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
    /// Fast BBO-only broadcast path - bypasses expensive L2 snapshot computation
    BboUpdate {
        bbos: HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
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

    // Start parallel file watchers (crossbeam channel)
    let (crossbeam_rx, _handles, _last_os, _last_fills, _last_diffs) = parallel::start_parallel_file_watchers(dir);

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
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = tokio::time::interval_at(start, Duration::from_secs(10));
    let mut l2_flush_ticker = tokio::time::interval_at(
        Instant::now() + Duration::from_millis(L2_FLUSH_TICK_MS),
        Duration::from_millis(L2_FLUSH_TICK_MS),
    );
    l2_flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut snapshot_fetch_pending = false;

    info!("Main event loop starting");

    loop {
        tokio::select! {
            biased;

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

            // Flush throttled L2 changes even if no further file event arrives.
            _ = l2_flush_ticker.tick() => {
                listener.lock().await.flush_l2_if_due();
            }

            // Snapshot fetch result
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                snapshot_fetch_pending = false;
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }

            // Periodic snapshot fetch (initial only)
            _ = ticker.tick() => {
                let is_ready = listener.lock().await.is_ready();
                info!("Ticker: is_ready={}, snapshot_fetch_pending={}", is_ready, snapshot_fetch_pending);
                if !is_ready && !snapshot_fetch_pending {
                    snapshot_fetch_pending = true;
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    fetch_snapshot(snapshot_config.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
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

    fn listener_with_btc_bids(tx: Sender<Arc<InternalMessage>>, bids: &[(u64, &str, &str)]) -> OrderBookListener {
        listener_with_coin_bids(tx, &[("BTC", bids)])
    }

    fn listener_with_coin_bids(
        tx: Sender<Arc<InternalMessage>>,
        books: &[(&str, &[(u64, &str, &str)])],
    ) -> OrderBookListener {
        let mut snapshot_map = HashMap::new();
        for (coin, bids) in books {
            let mut book = OrderBook::new();
            for (oid, px, sz) in *bids {
                book.add_order(inner_order(*oid, coin, Side::Bid, px, sz));
            }
            snapshot_map.insert(Coin::new(coin), book.to_snapshot());
        }

        let snapshots = Snapshots::new(snapshot_map);
        let mut listener = OrderBookListener::new(Some(tx), false);
        listener.init_from_snapshot(snapshots, 0);
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

    fn update_diff_line(coin: &str, oid: u64, orig_sz: &str, new_sz: &str) -> String {
        serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 1,
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

    fn drain_latest_l2(rx: &mut Receiver<Arc<InternalMessage>>) -> Option<Arc<InternalMessage>> {
        let mut latest = None;
        while let Ok(msg) = rx.try_recv() {
            if matches!(msg.as_ref(), InternalMessage::L2Update { .. }) {
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
