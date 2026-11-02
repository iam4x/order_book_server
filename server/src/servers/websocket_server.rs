use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use axum::{Router, extract::Query, response::IntoResponse, routing::get, serve::ListenerExt};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use prometheus::IntCounter;
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    select,
    sync::{
        OwnedSemaphorePermit, Semaphore,
        broadcast::{Sender, channel},
        mpsc, watch,
    },
};
use yawc::{FrameView, OpCode, WebSocket};

use crate::{
    FeatureSet, ServerConfig,
    listeners::order_book::{
        AllBboSubscriptionRegistry, InternalMessage, L2SubscriptionKey, L2SubscriptionRegistry, L4CoinSnapshot,
        LISTENER_MAILBOX_CAPACITY, ListenerClient, ListenerCommand, OrderBookListener, PayloadCache, PreparedL2Book,
        hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, Px, RawBbo, Sz},
    order_sync::{OrderSyncHub, OrderSyncStatus},
    prelude::*,
    types::{
        AllBbo, AllBboEntry, Bbo, L4Book, L4BookUpdates, L4Order, Level, Stats, Trade,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, OrderUpdate, ServerResponse, Subscription, SubscriptionManager},
    },
};

#[derive(Debug, Deserialize)]
struct WsAuthQuery {
    token: Option<String>,
}

/// Cached children of `BROADCASTS_TOTAL`, so the per-message path skips the label lookup.
struct BroadcastCounters {
    l2: IntCounter,
    l2_heartbeat: IntCounter,
    bbo: IntCounter,
    bbo_heartbeat: IntCounter,
    allbbo: IntCounter,
    allbbo_heartbeat: IntCounter,
    trades: IntCounter,
    l4: IntCounter,
    book_diffs: IntCounter,
    stats: IntCounter,
    order_sync: IntCounter,
}

static BROADCASTS: LazyLock<BroadcastCounters> = LazyLock::new(|| {
    let counter = |channel: &str| BROADCASTS_TOTAL.with_label_values(&[channel]);
    BroadcastCounters {
        l2: counter("l2"),
        l2_heartbeat: counter("l2_heartbeat"),
        bbo: counter("bbo"),
        bbo_heartbeat: counter("bbo_heartbeat"),
        allbbo: counter("allbbo"),
        allbbo_heartbeat: counter("allbbo_heartbeat"),
        trades: counter("trades"),
        l4: counter("l4"),
        book_diffs: counter("bookDiffs"),
        stats: counter("stats"),
        order_sync: counter("orderSync"),
    }
});

/// Per-subscription cached L2 broadcast. The prepared book's version is used for
/// change-based dedup; its payload is resent (with refreshed `time`) when the heartbeat fires.
struct L2Entry {
    last_sent: Instant,
    prepared: Arc<PreparedL2Book>,
}

/// Per-coin cached BBO broadcast. `raw` is used for change-based dedup and is resent
/// (with refreshed `time`) when the heartbeat fires.
struct BboEntry {
    raw: RawBbo,
    last_sent: Instant,
}

/// Heights an L4 snapshot already covers for its coin. Buffered broadcast updates at or
/// below them predate the snapshot and are dropped instead of applied twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct L4UpdateCutoff {
    diffs_through: u64,
    statuses_through: u64,
}

impl L4UpdateCutoff {
    const fn from_snapshot(snapshot: &L4CoinSnapshot) -> Self {
        Self { diffs_through: snapshot.diffs_through_height, statuses_through: snapshot.statuses_through_height }
    }
}

#[derive(Clone, Copy)]
enum L4UpdateKind {
    Diffs,
    Statuses,
}

/// True when `subscription` is an L4 subscription whose snapshot already holds this update.
fn l4_update_in_snapshot(
    cutoffs: &HashMap<String, L4UpdateCutoff>,
    subscription: &Subscription,
    kind: L4UpdateKind,
    height: u64,
) -> bool {
    let Subscription::L4Book { coin } = subscription else { return false };
    cutoffs.get(coin).is_some_and(|cutoff| {
        height
            <= match kind {
                L4UpdateKind::Diffs => cutoff.diffs_through,
                L4UpdateKind::Statuses => cutoff.statuses_through,
            }
    })
}

#[derive(Default)]
struct AllBboCache {
    entries_by_coin: HashMap<String, AllBboEntry>,
    last_payload: Option<CachedAllBboPayload>,
}

struct CachedAllBboPayload {
    sent_at: Instant,
    payload: AllBbo,
}

impl AllBboCache {
    fn prime_snapshot(&mut self, payload: &AllBbo, sent_at: Instant) {
        self.entries_by_coin = payload.bbos.iter().cloned().map(|entry| (entry.coin.clone(), entry)).collect();
        self.last_payload = Some(CachedAllBboPayload { sent_at, payload: payload.clone() });
    }

    fn delta<I>(&mut self, time: u64, entries: I, sent_at: Instant) -> Option<AllBbo>
    where
        I: IntoIterator<Item = AllBboEntry>,
    {
        let mut changed = Vec::new();
        for entry in entries {
            match self.entries_by_coin.entry(entry.coin.clone()) {
                Entry::Occupied(cached) if cached.get() == &entry => {}
                Entry::Occupied(mut cached) => {
                    cached.insert(entry.clone());
                    changed.push(entry);
                }
                Entry::Vacant(cached) => {
                    cached.insert(entry.clone());
                    changed.push(entry);
                }
            }
        }

        if changed.is_empty() {
            return None;
        }

        let payload = AllBbo { time, bbos: changed };
        self.last_payload = Some(CachedAllBboPayload { sent_at, payload: payload.clone() });
        Some(payload)
    }

    fn heartbeat(&mut self, now: Instant, time: u64, interval: Duration) -> Option<AllBbo> {
        let cached = self.last_payload.as_mut()?;
        if now.duration_since(cached.sent_at) < interval {
            return None;
        }

        cached.sent_at = now;
        cached.payload.time = time;
        Some(cached.payload.clone())
    }
}

struct ConnectionL2Registrations {
    registry: Arc<L2SubscriptionRegistry>,
    subscriptions: HashSet<L2SubscriptionKey>,
}

impl ConnectionL2Registrations {
    fn new(registry: Arc<L2SubscriptionRegistry>) -> Self {
        Self { registry, subscriptions: HashSet::new() }
    }

    fn register(&mut self, subscription: &Subscription) {
        if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
            let key = L2SubscriptionKey::new(Coin::new(coin), *n_sig_figs, *mantissa, *n_levels);
            if self.subscriptions.insert(key.clone()) {
                self.registry.register_l2(key);
            }
        }
    }

    fn unregister(&mut self, subscription: &Subscription) {
        if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
            let key = L2SubscriptionKey::new(Coin::new(coin), *n_sig_figs, *mantissa, *n_levels);
            if self.subscriptions.remove(&key) {
                self.registry.unregister_l2(&key);
            }
        }
    }
}

struct ConnectionAllBboRegistration {
    registry: Arc<AllBboSubscriptionRegistry>,
    registered: bool,
}

impl ConnectionAllBboRegistration {
    fn new(registry: Arc<AllBboSubscriptionRegistry>) -> Self {
        Self { registry, registered: false }
    }

    fn register(&mut self, subscription: &Subscription) {
        if matches!(subscription, Subscription::AllBbo) && !self.registered {
            self.registry.register();
            self.registered = true;
        }
    }

    fn unregister(&mut self, subscription: &Subscription) {
        if matches!(subscription, Subscription::AllBbo) && self.registered {
            self.registry.unregister();
            self.registered = false;
        }
    }
}

impl Drop for ConnectionAllBboRegistration {
    fn drop(&mut self) {
        if self.registered {
            self.registry.unregister();
        }
    }
}

impl Drop for ConnectionL2Registrations {
    fn drop(&mut self) {
        for key in self.subscriptions.drain() {
            self.registry.unregister_l2(&key);
        }
    }
}

/// Build a tokio interval that fires often enough to drive configured heartbeats with
/// at most half the configured period of drift. Returns None when all heartbeats are disabled.
fn build_heartbeat_ticker(
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    allbbo_heartbeat_ms: u64,
) -> Option<tokio::time::Interval> {
    let enabled =
        [l2book_heartbeat_ms, bbo_heartbeat_ms, allbbo_heartbeat_ms].into_iter().filter(|&ms| ms > 0).min()?;
    let tick_ms = (enabled / 2).max(50).min(500);
    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    Some(interval)
}

/// Await the next heartbeat tick, or pend forever when no heartbeat is configured.
async fn heartbeat_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn websocket_token_authorized(secret: Option<&str>, token: Option<&str>) -> bool {
    match secret {
        Some(secret) => token == Some(secret),
        None => true,
    }
}

pub async fn run_websocket_server(config: ServerConfig) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(16384);
    let order_sync = config.features.ordersync().then(OrderSyncHub::spawn);

    // Market filter flags from config
    let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
    let ignore_spot = !config.include_spot; // For OrderBookListener (legacy)
    let compression_level = config.compression_level;

    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        let mut listener = OrderBookListener::new(Some(internal_message_tx), ignore_spot, config.features);
        if let Some(order_sync) = &order_sync {
            listener.set_order_sync_recorder(order_sync.recorder());
        }
        listener
    };
    let l2_subscription_registry = listener.l2_subscription_registry();
    let allbbo_subscription_registry = listener.allbbo_subscription_registry();
    let (command_tx, command_rx) = mpsc::channel::<ListenerCommand>(LISTENER_MAILBOX_CAPACITY);
    let listener_client = ListenerClient::new(command_tx.clone(), listener.subscribe_status());
    {
        let config = config.clone();
        tokio::spawn(async move {
            info!("Starting HFT-optimized listener");
            let result = hl_listen_hft(listener, command_rx, command_tx, config).await;
            if let Err(err) = result {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts = if compression_level == 0 {
        yawc::Options::default()
    } else {
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level))
    };
    let websocket_secret = config.secret.as_deref().map(Arc::<str>::from);

    let start_time = Instant::now();
    let listener_for_health = listener_client.clone();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let l2book_heartbeat_ms = config.l2book_heartbeat_ms;
                let bbo_heartbeat_ms = config.bbo_heartbeat_ms;
                let allbbo_heartbeat_ms = config.allbbo_heartbeat_ms;
                let features = config.features;
                let listener = listener_client.clone();
                let l2_subscription_registry = Arc::clone(&l2_subscription_registry);
                let allbbo_subscription_registry = Arc::clone(&allbbo_subscription_registry);
                let websocket_secret = websocket_secret.clone();
                let order_sync = order_sync.clone();
                move |Query(query): Query<WsAuthQuery>, ws_upgrade| {
                    let websocket_secret = websocket_secret.clone();
                    let order_sync = order_sync.clone();
                    async move {
                        if !websocket_token_authorized(websocket_secret.as_deref(), query.token.as_deref()) {
                            return (axum::http::StatusCode::UNAUTHORIZED, "unauthorized websocket connection")
                                .into_response();
                        }

                        ws_handler(
                            ws_upgrade,
                            internal_message_tx.clone(),
                            listener.clone(),
                            Arc::clone(&l2_subscription_registry),
                            Arc::clone(&allbbo_subscription_registry),
                            market_filter,
                            features,
                            l2book_heartbeat_ms,
                            bbo_heartbeat_ms,
                            allbbo_heartbeat_ms,
                            order_sync,
                            websocket_opts,
                        )
                    }
                }
            }),
        )
        .route(
            "/health",
            get(move || {
                let listener = listener_for_health.clone();
                async move {
                    let is_ready = listener.status().ready;
                    let uptime_secs = start_time.elapsed().as_secs();
                    let height = ORDERBOOK_HEIGHT.get();
                    let connections = WS_CONNECTIONS_ACTIVE.get();
                    let body = format!(
                        r#"{{"status":"{}","uptime_seconds":{},"height":{},"connections":{}}}"#,
                        if is_ready { "ready" } else { "initializing" },
                        uptime_secs,
                        height,
                        connections,
                    );
                    axum::response::Response::builder().header("content-type", "application/json").body(body).unwrap()
                }
            }),
        );

    let tcp_listener = TcpListener::bind(&config.address).await?.tap_io(|tcp_stream| {
        if let Err(err) = tcp_stream.set_nodelay(true) {
            log::warn!("failed to set TCP_NODELAY on incoming connection: {err}");
        }
    });
    info!("WebSocket server running at ws://{}", config.address);

    if let Err(err) = axum::serve(tcp_listener, app).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: ListenerClient,
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    allbbo_subscription_registry: Arc<AllBboSubscriptionRegistry>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    features: FeatureSet,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    allbbo_heartbeat_ms: u64,
    order_sync: Option<Arc<OrderSyncHub>>,
    websocket_opts: yawc::Options,
) -> axum::response::Response {
    // Reject malformed WS handshakes cleanly. The previous `.unwrap()` would panic
    // inside the axum handler task and dump a backtrace per request.
    let (resp, fut) = match incoming.upgrade(websocket_opts) {
        Ok(pair) => pair,
        Err(err) => {
            log::warn!("rejecting malformed websocket upgrade: {err}");
            return (axum::http::StatusCode::BAD_REQUEST, "invalid websocket upgrade").into_response();
        }
    };
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(
            ws,
            internal_message_tx,
            listener,
            l2_subscription_registry,
            allbbo_subscription_registry,
            market_filter,
            features,
            l2book_heartbeat_ms,
            bbo_heartbeat_ms,
            allbbo_heartbeat_ms,
            order_sync,
        )
        .await
    });

    resp.into_response()
}

#[cfg(test)]
mod auth_tests {
    use super::websocket_token_authorized;

    #[test]
    fn no_secret_allows_missing_token() {
        assert!(websocket_token_authorized(None, None));
    }

    #[test]
    fn no_secret_allows_any_token() {
        assert!(websocket_token_authorized(None, Some("anything")));
    }

    #[test]
    fn secret_accepts_exact_token() {
        assert!(websocket_token_authorized(Some("secret"), Some("secret")));
    }

    #[test]
    fn secret_rejects_missing_token() {
        assert!(!websocket_token_authorized(Some("secret"), None));
    }

    #[test]
    fn secret_rejects_mismatched_token() {
        assert!(!websocket_token_authorized(Some("secret"), Some("wrong")));
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_socket(
    socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: ListenerClient,
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    allbbo_subscription_registry: Arc<AllBboSubscriptionRegistry>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    features: FeatureSet,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    allbbo_heartbeat_ms: u64,
    order_sync: Option<Arc<OrderSyncHub>>,
) {
    // Track connection metrics
    WS_CONNECTIONS_ACTIVE.inc();
    WS_CONNECTIONS_TOTAL.inc();

    // Use a guard to decrement active connections when this function exits
    struct ConnectionGuard;
    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            WS_CONNECTIONS_ACTIVE.dec();
            BROADCAST_RECEIVERS.dec();
        }
    }
    let _connection_guard = ConnectionGuard;

    let mut internal_message_rx = internal_message_tx.subscribe();
    BROADCAST_RECEIVERS.set(internal_message_tx.receiver_count() as i64);
    // Read after subscribing: the actor publishes the status before it broadcasts a universe change.
    let status = listener.status();
    let is_ready = status.ready;
    let mut manager = SubscriptionManager::default();
    let mut universe = filter_universe(&status.universe, market_filter.0, market_filter.1, market_filter.2);
    // Per-subscription cache for L2 dedup + heartbeat resend.
    let mut last_l2: HashMap<L2SubscriptionKey, L2Entry> = HashMap::new();
    // Per-coin cache for BBO dedup + heartbeat resend
    let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
    let mut last_allbbo = AllBboCache::default();
    let mut l4_cutoffs: HashMap<String, L4UpdateCutoff> = HashMap::new();
    let mut l2_registrations = ConnectionL2Registrations::new(l2_subscription_registry);
    let mut allbbo_registration = ConnectionAllBboRegistration::new(allbbo_subscription_registry);
    let mut order_sync_rx: Option<watch::Receiver<OrderSyncStatus>> = None;
    let (mut ws_read, outbound, writer) = spawn_writer(socket);
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        let _unused = outbound.send_message(msg);
        close_writer(outbound, writer).await;
        return;
    }

    // Optional heartbeat ticker. We tick at min(enabled_heartbeats)/2 (clamped to [50, 500] ms)
    // so each subscription's last-sent timestamp can drift at most half a heartbeat from the configured value.
    let effective_l2book_heartbeat_ms = if features.l2book() { l2book_heartbeat_ms } else { 0 };
    let effective_bbo_heartbeat_ms = if features.bbo() { bbo_heartbeat_ms } else { 0 };
    let effective_allbbo_heartbeat_ms = if features.allbbo() { allbbo_heartbeat_ms } else { 0 };
    let mut heartbeat_ticker = build_heartbeat_ticker(
        effective_l2book_heartbeat_ms,
        effective_bbo_heartbeat_ms,
        effective_allbbo_heartbeat_ms,
    );
    let l2_hb = if effective_l2book_heartbeat_ms > 0 {
        Some(Duration::from_millis(effective_l2book_heartbeat_ms))
    } else {
        None
    };
    let bbo_hb =
        if effective_bbo_heartbeat_ms > 0 { Some(Duration::from_millis(effective_bbo_heartbeat_ms)) } else { None };
    let allbbo_hb = if effective_allbbo_heartbeat_ms > 0 {
        Some(Duration::from_millis(effective_allbbo_heartbeat_ms))
    } else {
        None
    };

    let mut alive = true;
    while alive {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::L2Update{ l2_books } => {
                                if !features.l2book() {
                                    continue;
                                }
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    // Partial L2 updates intentionally omit unchanged coins.
                                    if matches!(sub, Subscription::L2Book { .. }) {
                                        alive &= send_ws_data_from_l2_update(&outbound, sub, l2_books, &mut last_l2);
                                    }
                                }
                            },
                            InternalMessage::Universe{ universe: next_universe } => {
                                universe = filter_universe(next_universe, market_filter.0, market_filter.1, market_filter.2);
                            },
                            InternalMessage::BboUpdate{ bbos, time, payloads } => {
                                if !features.bbo() {
                                    continue;
                                }
                                // Fast path for BBO subscribers only
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    if let Subscription::Bbo { coin } = sub {
                                        alive &= send_ws_data_from_bbo(&outbound, coin, bbos, *time, payloads, &mut last_bbo);
                                    }
                                }
                            },
                            InternalMessage::AllBboUpdate{ bbos, time } => {
                                if !features.allbbo() || !manager.subscriptions().contains(&Subscription::AllBbo) {
                                    continue;
                                }
                                alive &= send_ws_data_from_allbbo(
                                    &outbound,
                                    bbos,
                                    *time,
                                    market_filter,
                                    &mut last_allbbo,
                                );
                            },
                            InternalMessage::Fills{ batch, trades } => {
                                if !features.trades() {
                                    continue;
                                }
                                let has_trades = manager.subscriptions().iter().any(|s| matches!(s, Subscription::Trades { .. }));
                                if has_trades {
                                    let trades = trades.get_or_build(|| trade_payloads(batch));
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        if let Subscription::Trades { coin } = sub {
                                            alive &= send_coin_payload(&outbound, trades, coin, &BROADCASTS.trades);
                                        }
                                    }
                                }
                            },
                            InternalMessage::L4OrderDiffs{ batch, l4_updates, raw_diffs } => {
                                if !features.l4book() && !features.bookdiffs() {
                                    continue;
                                }
                                let has_l4 = manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. }));
                                let has_book_diffs = manager.subscriptions().iter().any(|s| matches!(s, Subscription::BookDiffs { .. }));
                                if has_l4 || has_book_diffs {
                                    let book_updates = has_l4.then(|| l4_updates.get_or_build(|| l4_diff_payloads(batch)));
                                    let raw_diffs = has_book_diffs.then(|| raw_diffs.get_or_build(|| raw_diff_payloads(batch)));
                                    let height = batch.block_number();
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        if let (Some(updates), Subscription::L4Book { coin }) = (book_updates, sub)
                                            && !l4_update_in_snapshot(&l4_cutoffs, sub, L4UpdateKind::Diffs, height)
                                        {
                                            alive &= send_coin_payload(&outbound, updates, coin, &BROADCASTS.l4);
                                        }
                                        if !alive { break; }
                                        if let (Some(diffs), Subscription::BookDiffs { coin }) = (raw_diffs, sub) {
                                            alive &= send_coin_payload(&outbound, diffs, coin, &BROADCASTS.book_diffs);
                                        }
                                    }
                                }
                            },
                            InternalMessage::L4OrderStatuses{ batch, l4_updates, order_updates } => {
                                if !features.l4book() && !features.orderupdates() {
                                    continue;
                                }
                                let has_l4 = manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. }));
                                let has_order_updates = manager.subscriptions().iter().any(|s| matches!(s, Subscription::OrderUpdates { .. }));
                                if has_l4 {
                                    let book_updates = l4_updates.get_or_build(|| l4_status_payloads(batch));
                                    let height = batch.block_number();
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        if let Subscription::L4Book { coin } = sub
                                            && !l4_update_in_snapshot(&l4_cutoffs, sub, L4UpdateKind::Statuses, height)
                                        {
                                            alive &= send_coin_payload(&outbound, book_updates, coin, &BROADCASTS.l4);
                                        }
                                    }
                                }
                                if has_order_updates {
                                    let order_updates = order_updates.get_or_build(|| order_update_payloads(batch));
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        alive &= send_ws_order_updates(&outbound, sub, order_updates);
                                    }
                                }
                            },
                            InternalMessage::Stats{ stats } => {
                                if !features.stats() || !manager.subscriptions().contains(&Subscription::Stats) {
                                    continue;
                                }
                                alive &= send_ws_stats(&outbound, stats.clone());
                            },
                        }

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        report_broadcast_lag(&outbound, n);
                        break;
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        break;
                    }
                }
            }

            _ = heartbeat_tick(&mut heartbeat_ticker) => {
                let now = Instant::now();
                let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                for sub in manager.subscriptions() {
                    if !alive { break; }
                    match sub {
                        Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
                            let Some(hb) = l2_hb else { continue };
                            let key = L2SubscriptionKey::new(Coin::new(coin), *n_sig_figs, *mantissa, *n_levels);
                            if let Some(entry) = last_l2.get_mut(&key)
                                && now.duration_since(entry.last_sent) >= hb
                            {
                                entry.last_sent = now;
                                BROADCASTS.l2_heartbeat.inc();
                                let mut payload = entry.prepared.payload().clone();
                                payload.set_time(now_ms);
                                alive &= outbound.send_message(ServerResponse::L2Book(payload));
                            }
                        }
                        Subscription::Bbo { coin } => {
                            let Some(hb) = bbo_hb else { continue };
                            if let Some(entry) = last_bbo.get_mut(coin)
                                && now.duration_since(entry.last_sent) >= hb
                            {
                                entry.last_sent = now;
                                BROADCASTS.bbo_heartbeat.inc();
                                alive &= outbound.send_message(ServerResponse::Bbo(bbo_from_raw(coin, now_ms, entry.raw)));
                            }
                        }
                        Subscription::AllBbo => {
                            let Some(hb) = allbbo_hb else { continue };
                            let Some(payload) = last_allbbo.heartbeat(now, now_ms, hb) else { continue };
                            BROADCASTS.allbbo_heartbeat.inc();
                            alive &= outbound.send_message(ServerResponse::AllBbo(payload));
                        }
                        _ => {}
                    }
                }
            }

            status = receive_order_sync(&mut order_sync_rx) => {
                BROADCASTS.order_sync.inc();
                alive &= outbound.send_message(ServerResponse::OrderSync(status));
            }

            msg = ws_read.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    break;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::Ping => {
                                        alive &= outbound.send_pong();
                                    }
                                    _ => {
                                        alive &= receive_client_message(
                                            &outbound,
                                            &mut manager,
                                            value,
                                            &universe,
                                            &listener,
                                            features,
                                            market_filter,
                                            &mut last_l2,
                                            &mut last_bbo,
                                            &mut last_allbbo,
                                            &mut l4_cutoffs,
                                            &mut l2_registrations,
                                            &mut allbbo_registration,
                                            order_sync.as_deref(),
                                            &mut order_sync_rx,
                                        ).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                alive &= outbound.send_message(msg);
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            break;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    break;
                }
            }

            _ = outbound.data_tx.closed() => {
                break;
            }
        }
    }
    info!("Dropping connection: outbound writer closed or queue limit reached");
    close_writer(outbound, writer).await;
}

#[allow(clippy::too_many_arguments)]
async fn receive_client_message(
    outbound: &Outbound,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: &ListenerClient,
    features: FeatureSet,
    market_filter: (bool, bool, bool),
    last_l2: &mut HashMap<L2SubscriptionKey, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    last_allbbo: &mut AllBboCache,
    l4_cutoffs: &mut HashMap<String, L4UpdateCutoff>,
    l2_registrations: &mut ConnectionL2Registrations,
    allbbo_registration: &mut ConnectionAllBboRegistration,
    order_sync: Option<&OrderSyncHub>,
    order_sync_rx: &mut Option<watch::Receiver<OrderSyncStatus>>,
) -> bool {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    if !subscription_feature_enabled(&subscription, features) {
        return outbound.send_message(ServerResponse::Error(format!(
            "Feature disabled for subscription type: {}",
            subscription.type_label()
        )));
    }
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !validate_subscription_for_features(&subscription, universe, features, market_filter) {
        return outbound.send_message(ServerResponse::Error(format!("Invalid subscription: {sub}")));
    }

    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => match manager.subscribe(subscription.clone()) {
            Ok(inserted) => ("", inserted),
            Err(err) => {
                return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}")));
            }
        },
        ClientMessage::Unsubscribe { .. } => {
            let removed = manager.unsubscribe(subscription.clone());
            // Drop the per-connection dedup/heartbeat cache entry for the just-unsubscribed
            // stream. Without this, a client that sub/unsub-cycles distinct L2 variants on
            // the same coin (or BBO across coins) leaks one entry per cycle until disconnect.
            if removed {
                match &subscription {
                    Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
                        let key = L2SubscriptionKey::new(Coin::new(coin), *n_sig_figs, *mantissa, *n_levels);
                        last_l2.remove(&key);
                        l2_registrations.unregister(&subscription);
                    }
                    Subscription::Bbo { coin } => {
                        last_bbo.remove(coin);
                    }
                    Subscription::AllBbo => {
                        *last_allbbo = AllBboCache::default();
                        allbbo_registration.unregister(&subscription);
                    }
                    Subscription::L4Book { coin } => {
                        l4_cutoffs.remove(coin);
                    }
                    _ => {}
                }
            }
            ("un", removed)
        }
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        match &client_message {
            ClientMessage::Subscribe { subscription: Subscription::OrderSync } => {
                *order_sync_rx = order_sync.map(OrderSyncHub::subscribe);
            }
            ClientMessage::Unsubscribe { subscription: Subscription::OrderSync } => {
                *order_sync_rx = None;
            }
            _ => {}
        }
        if let ClientMessage::Subscribe { subscription } = &client_message {
            l2_registrations.register(subscription);
            allbbo_registration.register(subscription);
        }
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener, market_filter, l4_cutoffs).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    l2_registrations.unregister(subscription);
                    allbbo_registration.unregister(subscription);
                    return outbound
                        .send_message(ServerResponse::Error(format!("Unable to grab order book snapshot: {err}")));
                }
            }
        } else {
            None
        };
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)) {
            return false;
        }
        if let Some(snapshot_msg) = snapshot_msg {
            if let ServerResponse::AllBbo(payload) = &snapshot_msg {
                last_allbbo.prime_snapshot(payload, Instant::now());
            }
            return outbound.send_message(snapshot_msg);
        }
        true
    } else {
        outbound.send_message(ServerResponse::Error(format!("Already {word}subscribed: {sub}")))
    }
}

/// The connection missed `skipped` broadcasts. Every stream is incremental or deduplicated
/// against what this connection last sent, so it cannot recover in place: tell the client to
/// reconnect, and the caller closes the connection.
fn report_broadcast_lag(outbound: &Outbound, skipped: u64) {
    CHANNEL_LAG.set(i64::try_from(skipped).unwrap_or(i64::MAX));
    CHANNEL_DROPS_TOTAL.inc();
    log::warn!("Receiver lagged by {skipped} messages; closing the connection");
    let _unused = outbound.send_message(ServerResponse::Error(format!(
        "Stream lagged: {skipped} messages were dropped; reconnect to resubscribe"
    )));
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation.
/// Returns false if outbound admission fails (caller must drop the connection).
fn send_ws_data_from_bbo(
    outbound: &Outbound,
    coin: &str,
    bbos: &HashMap<Coin, RawBbo>,
    time: u64,
    payloads: &PayloadCache<String>,
    last_bbo: &mut HashMap<String, BboEntry>,
) -> bool {
    let coin_key = Coin::new(coin);
    let Some(raw) = bbos.get(&coin_key).copied() else { return true };
    // The level count is not part of the dedup key: only price and size changes are sent.
    if last_bbo.get(coin).is_some_and(|entry| bbo_dedup_key(entry.raw) == bbo_dedup_key(raw)) {
        return true;
    }
    BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
    BROADCASTS.bbo.inc();
    last_bbo.insert(coin.to_string(), BboEntry { raw, last_sent: Instant::now() });
    let payloads = payloads.get_or_build(|| bbo_payloads(bbos, time));
    payloads.get(coin).is_none_or(|payload| outbound.send_payload(payload.clone()))
}

type BboDedupKey = (Option<(Px, Sz)>, Option<(Px, Sz)>);

const fn bbo_dedup_key(raw: RawBbo) -> BboDedupKey {
    const fn side(level: Option<(Px, Sz, u32)>) -> Option<(Px, Sz)> {
        match level {
            Some((px, sz, _n)) => Some((px, sz)),
            None => None,
        }
    }
    (side(raw.0), side(raw.1))
}

fn bbo_from_raw(coin: &str, time: u64, raw: RawBbo) -> Bbo {
    let (bid, ask) = levels_from_raw_bbo(raw);
    Bbo { coin: coin.to_string(), time, bid, ask }
}

fn bbo_payloads(bbos: &HashMap<Coin, RawBbo>, time: u64) -> HashMap<String, bytes::Bytes> {
    bbos.iter()
        .filter_map(|(coin, raw)| {
            let coin = coin.value();
            let payload = serialize_response(&ServerResponse::Bbo(bbo_from_raw(&coin, time, *raw)))?;
            Some((coin, payload))
        })
        .collect()
}

fn send_ws_data_from_allbbo(
    outbound: &Outbound,
    bbos: &[(Coin, RawBbo)],
    time: u64,
    market_filter: (bool, bool, bool),
    last_allbbo: &mut AllBboCache,
) -> bool {
    let entries = bbos
        .iter()
        .filter(|(coin, _)| market_filter_allows_coin_ref(coin, market_filter))
        .map(|(coin, raw_bbo)| allbbo_entry_from_raw(coin, *raw_bbo));
    let Some(payload) = last_allbbo.delta(time, entries, Instant::now()) else { return true };

    BROADCASTS.allbbo.inc();
    outbound.send_message(ServerResponse::AllBbo(payload))
}

fn level_from_raw(raw: Option<(Px, Sz, u32)>) -> Option<Level> {
    raw.map(|(px, sz, n)| Level::new(px.to_str(), sz.to_str(), n as usize))
}

fn levels_from_raw_bbo(raw_bbo: RawBbo) -> (Option<Level>, Option<Level>) {
    (level_from_raw(raw_bbo.0), level_from_raw(raw_bbo.1))
}

fn allbbo_entry_from_raw(coin: &Coin, raw_bbo: RawBbo) -> AllBboEntry {
    AllBboEntry {
        coin: coin.value(),
        bid: raw_bbo.0.map(|(px, _sz, _n)| px.to_str()),
        ask: raw_bbo.1.map(|(px, _sz, _n)| px.to_str()),
    }
}

const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const OUTBOUND_QUEUE_DEPTH: usize = 128;
const OUTBOUND_BYTE_BUDGET: usize = 16 * 1024 * 1024;
const PONG_QUEUE_DEPTH: usize = 8;
const PONG_FRAME: &[u8] = br#"{"channel":"pong"}"#;

/// Serialize a response to its wire bytes; a failure is logged and the response is skipped.
fn serialize_response(msg: &ServerResponse) -> Option<bytes::Bytes> {
    match serde_json::to_string(msg) {
        Ok(payload) => Some(bytes::Bytes::from(payload)),
        Err(err) => {
            error!("Server response serialization error: {err}");
            None
        }
    }
}

type WsRead = futures_util::stream::SplitStream<WebSocket>;
type WsWriter = tokio::task::JoinHandle<()>;

struct QueuedData {
    payload: bytes::Bytes,
    budget: OwnedSemaphorePermit,
}

struct Outbound {
    data_tx: mpsc::Sender<QueuedData>,
    data_budget: Arc<Semaphore>,
    pong_tx: mpsc::Sender<bytes::Bytes>,
}

impl Outbound {
    fn new(data_tx: mpsc::Sender<QueuedData>, pong_tx: mpsc::Sender<bytes::Bytes>) -> Self {
        Self { data_tx, data_budget: Arc::new(Semaphore::new(OUTBOUND_BYTE_BUDGET)), pong_tx }
    }

    fn send_message(&self, msg: ServerResponse) -> bool {
        serialize_response(&msg).is_none_or(|payload| self.send_payload(payload))
    }

    fn send_payload(&self, payload: bytes::Bytes) -> bool {
        let max_permits = u32::try_from(OUTBOUND_BYTE_BUDGET).unwrap_or(u32::MAX);
        let permits = u32::try_from(payload.len()).unwrap_or(u32::MAX).clamp(1, max_permits);
        let Ok(budget) = Arc::clone(&self.data_budget).try_acquire_many_owned(permits) else {
            error!("Outbound byte budget exhausted; dropping slow client");
            WS_SEND_ERRORS_TOTAL.inc();
            return false;
        };
        let frame = QueuedData { payload, budget };
        match self.data_tx.try_send(frame) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                error!("Outbound queue full; dropping slow client");
                WS_SEND_ERRORS_TOTAL.inc();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    fn send_pong(&self) -> bool {
        match self.pong_tx.try_send(bytes::Bytes::from_static(PONG_FRAME)) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                error!("Pong queue full; dropping slow client");
                WS_SEND_ERRORS_TOTAL.inc();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

fn spawn_writer(socket: WebSocket) -> (WsRead, Outbound, WsWriter) {
    let (sink, ws_read) = socket.split();
    let (data_tx, data_rx) = mpsc::channel(OUTBOUND_QUEUE_DEPTH);
    let (pong_tx, pong_rx) = mpsc::channel(PONG_QUEUE_DEPTH);
    let writer = tokio::spawn(write_task(sink, data_rx, pong_rx));
    (ws_read, Outbound::new(data_tx, pong_tx), writer)
}

async fn write_task<S>(mut sink: S, mut data_rx: mpsc::Receiver<QueuedData>, mut pong_rx: mpsc::Receiver<bytes::Bytes>)
where
    S: futures_util::Sink<FrameView> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut data_open = true;
    let mut pong_open = true;
    while data_open || pong_open {
        let (payload, _budget) = select! {
            biased;

            pong = pong_rx.recv(), if pong_open => match pong {
                Some(payload) => (payload, None),
                None => {
                    pong_open = false;
                    continue;
                }
            },
            data = data_rx.recv(), if data_open => match data {
                Some(frame) => (frame.payload, Some(frame.budget)),
                None => {
                    data_open = false;
                    continue;
                }
            },
        };

        match tokio::time::timeout(WS_SEND_TIMEOUT, sink.send(FrameView::text(payload))).await {
            Ok(Ok(())) => {
                MESSAGES_SENT_TOTAL.inc();
            }
            Ok(Err(err)) => {
                error!("Failed to send: {err}");
                WS_SEND_ERRORS_TOTAL.inc();
                break;
            }
            Err(_) => {
                error!("Send timeout (>{WS_SEND_TIMEOUT:?}); dropping slow client");
                WS_SEND_ERRORS_TOTAL.inc();
                break;
            }
        }
    }

    let _unused = tokio::time::timeout(Duration::from_secs(1), sink.close()).await;
}

async fn close_writer(outbound: Outbound, mut writer: WsWriter) {
    drop(outbound);
    if tokio::time::timeout(WS_SEND_TIMEOUT, &mut writer).await.is_err() {
        writer.abort();
    }
}

// Filters coins based on market type flags
fn filter_universe(
    universe: &HashSet<Coin>,
    include_perps: bool,
    include_spot: bool,
    include_hip3: bool,
) -> HashSet<String> {
    universe
        .iter()
        .filter(|coin| market_filter_allows_coin_ref(coin, (include_perps, include_spot, include_hip3)))
        .map(Coin::value)
        .collect()
}

fn subscription_feature_enabled(subscription: &Subscription, features: FeatureSet) -> bool {
    match subscription {
        Subscription::Bbo { .. } => features.bbo(),
        Subscription::AllBbo => features.allbbo(),
        Subscription::L2Book { .. } => features.l2book(),
        Subscription::L4Book { .. } => features.l4book(),
        Subscription::Trades { .. } => features.trades(),
        Subscription::OrderUpdates { .. } => features.orderupdates(),
        Subscription::BookDiffs { .. } => features.bookdiffs(),
        Subscription::Stats => features.stats(),
        Subscription::OrderSync => features.ordersync(),
    }
}

fn market_filter_allows_coin(coin: &str, market_filter: (bool, bool, bool)) -> bool {
    if coin.is_empty() {
        return false;
    }
    let coin = Coin::new(coin);
    market_filter_allows_coin_ref(&coin, market_filter)
}

fn market_filter_allows_coin_ref(coin: &Coin, market_filter: (bool, bool, bool)) -> bool {
    (coin.is_perp() && market_filter.0) || (coin.is_spot() && market_filter.1) || (coin.is_hip3() && market_filter.2)
}

fn validate_subscription_for_features(
    subscription: &Subscription,
    universe: &HashSet<String>,
    features: FeatureSet,
    market_filter: (bool, bool, bool),
) -> bool {
    if features.requires_book_state() || !universe.is_empty() {
        return subscription.validate(universe);
    }

    match subscription {
        Subscription::Stats | Subscription::OrderSync | Subscription::AllBbo => true,
        Subscription::Trades { coin } | Subscription::BookDiffs { coin } => {
            market_filter_allows_coin(coin, market_filter)
        }
        Subscription::OrderUpdates { .. } => subscription.validate(universe),
        Subscription::Bbo { .. } | Subscription::L2Book { .. } | Subscription::L4Book { .. } => false,
    }
}

async fn receive_order_sync(receiver: &mut Option<watch::Receiver<OrderSyncStatus>>) -> OrderSyncStatus {
    let Some(receiver) = receiver else {
        return std::future::pending().await;
    };
    loop {
        if receiver.changed().await.is_ok() {
            return receiver.borrow_and_update().clone();
        }
        std::future::pending::<()>().await;
    }
}

fn send_ws_data_from_l2_update(
    outbound: &Outbound,
    subscription: &Subscription,
    l2_books: &HashMap<L2SubscriptionKey, Arc<PreparedL2Book>>,
    last_l2: &mut HashMap<L2SubscriptionKey, L2Entry>,
) -> bool {
    let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription else {
        return true;
    };

    let key = L2SubscriptionKey::new(Coin::new(coin), *n_sig_figs, *mantissa, *n_levels);
    let Some(prepared) = l2_books.get(&key) else {
        return true;
    };

    if last_l2.get(&key).map(|entry| entry.prepared.version()) == Some(prepared.version()) {
        return true;
    }

    BROADCASTS.l2.inc();
    last_l2.insert(key, L2Entry { last_sent: Instant::now(), prepared: Arc::clone(prepared) });
    prepared
        .wire_payload(|payload| serialize_response(&ServerResponse::L2Book(payload.clone())))
        .is_none_or(|payload| outbound.send_payload(payload.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    #[derive(Clone, Default)]
    struct RecordingSink(Arc<std::sync::Mutex<Vec<bytes::Bytes>>>);

    impl futures_util::Sink<FrameView> for RecordingSink {
        type Error = std::convert::Infallible;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: FrameView) -> std::result::Result<(), Self::Error> {
            self.0.lock().unwrap().push(item.payload);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct StuckSink;

    impl futures_util::Sink<FrameView> for StuckSink {
        type Error = std::convert::Infallible;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: FrameView) -> std::result::Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn raw_bbo(bid: Option<(&str, &str, u32)>, ask: Option<(&str, &str, u32)>) -> RawBbo {
        let parse = |(px, sz, n): (&str, &str, u32)| {
            (Px::parse_from_str(px).expect("valid px"), Sz::parse_from_str(sz).expect("valid sz"), n)
        };
        (bid.map(parse), ask.map(parse))
    }

    fn outbound_for_test() -> (Outbound, mpsc::Receiver<QueuedData>) {
        let (data_tx, data_rx) = mpsc::channel(8);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        (Outbound::new(data_tx, pong_tx), data_rx)
    }

    #[test]
    fn l2_registration_counts_distinct_levels_independently() {
        let registry = Arc::new(L2SubscriptionRegistry::default());
        let mut registrations = ConnectionL2Registrations::new(Arc::clone(&registry));
        let default_key = L2SubscriptionKey::new(Coin::new("BTC"), None, None, None);
        let ten_levels_key = L2SubscriptionKey::new(Coin::new("BTC"), None, None, Some(10));

        let default_levels =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: None, mantissa: None };
        let ten_levels =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: Some(10), mantissa: None };

        registrations.register(&default_levels);
        registrations.register(&ten_levels);
        registrations.register(&ten_levels);
        let mut other_connection = ConnectionL2Registrations::new(Arc::clone(&registry));
        other_connection.register(&default_levels);
        registrations.unregister(&default_levels);

        assert!(registry.active_keys().contains(&default_key));
        drop(other_connection);
        assert!(!registry.active_keys().contains(&default_key));
        assert!(registry.active_keys().contains(&ten_levels_key));

        registrations.unregister(&ten_levels);

        assert!(registry.active_keys().is_empty());

        registrations.register(&ten_levels);
        drop(registrations);
        assert!(registry.active_keys().is_empty());
    }

    #[test]
    fn filter_universe_applies_market_flags() {
        let universe = HashSet::from([Coin::new("BTC"), Coin::new("@1"), Coin::new("xyz:TSLA")]);

        let perps_hip3 = filter_universe(&universe, true, false, true);

        assert!(perps_hip3.contains("BTC"));
        assert!(perps_hip3.contains("xyz:TSLA"));
        assert!(!perps_hip3.contains("@1"));
    }

    #[test]
    fn subscription_feature_enabled_matches_feature_set() {
        let features: FeatureSet = "bbo,trades".parse().expect("valid features");

        assert!(subscription_feature_enabled(&Subscription::Bbo { coin: "BTC".to_string() }, features));
        assert!(subscription_feature_enabled(&Subscription::Trades { coin: "BTC".to_string() }, features));
        assert!(!subscription_feature_enabled(&Subscription::AllBbo, features));
        assert!(!subscription_feature_enabled(
            &Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: None, mantissa: None },
            features
        ));
        assert!(!subscription_feature_enabled(&Subscription::L4Book { coin: "BTC".to_string() }, features));
        assert!(!subscription_feature_enabled(&Subscription::BookDiffs { coin: "BTC".to_string() }, features));
        assert!(!subscription_feature_enabled(
            &Subscription::OrderUpdates { user: "0x0000000000000000000000000000000000000000".to_string() },
            features
        ));
        assert!(!subscription_feature_enabled(&Subscription::Stats, features));
        assert!(!subscription_feature_enabled(&Subscription::OrderSync, features));
        assert!(subscription_feature_enabled(&Subscription::Stats, "stats".parse().expect("valid features")));
        assert!(subscription_feature_enabled(&Subscription::OrderSync, "ordersync".parse().expect("valid features")));
    }

    #[test]
    fn raw_only_validation_does_not_require_universe() {
        let trades: FeatureSet = "trades".parse().expect("valid features");
        let bookdiffs: FeatureSet = "bookdiffs".parse().expect("valid features");
        let universe = HashSet::new();

        assert!(validate_subscription_for_features(
            &Subscription::Trades { coin: "BTC".to_string() },
            &universe,
            trades,
            (true, false, false),
        ));
        assert!(validate_subscription_for_features(
            &Subscription::BookDiffs { coin: "xyz:TSLA".to_string() },
            &universe,
            bookdiffs,
            (false, false, true),
        ));
        assert!(validate_subscription_for_features(
            &Subscription::OrderSync,
            &universe,
            "ordersync".parse().expect("valid features"),
            (false, false, false),
        ));
        assert!(!validate_subscription_for_features(
            &Subscription::Trades { coin: "@1".to_string() },
            &universe,
            trades,
            (true, false, false),
        ));
    }

    #[test]
    fn book_backed_validation_still_requires_universe() {
        let features: FeatureSet = "bbo,l2book".parse().expect("valid features");
        let universe = HashSet::new();

        assert!(!validate_subscription_for_features(
            &Subscription::Bbo { coin: "BTC".to_string() },
            &universe,
            features,
            (true, true, true),
        ));
    }

    #[test]
    fn allbbo_validation_follows_feature_gate_without_coin() {
        let allbbo: FeatureSet = "allbbo".parse().expect("valid features");
        let trades: FeatureSet = "trades".parse().expect("valid features");
        let universe = HashSet::from(["BTC".to_string()]);

        assert!(subscription_feature_enabled(&Subscription::AllBbo, allbbo));
        assert!(validate_subscription_for_features(&Subscription::AllBbo, &universe, allbbo, (true, true, true)));
        assert!(!subscription_feature_enabled(&Subscription::AllBbo, trades));
    }

    #[test]
    fn allbbo_send_path_compares_only_public_price_shape() {
        let mut cache = AllBboCache::default();
        cache.prime_snapshot(
            &AllBbo {
                time: 1,
                bbos: vec![AllBboEntry { coin: "BTC".to_string(), bid: Some("100".to_string()), ask: None }],
            },
            Instant::now(),
        );
        let (outbound, mut data_rx) = outbound_for_test();

        assert!(send_ws_data_from_allbbo(
            &outbound,
            &[(Coin::new("BTC"), raw_bbo(Some(("100", "2", 9)), None))],
            2,
            (true, true, true),
            &mut cache,
        ));
        assert!(matches!(data_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        assert!(send_ws_data_from_allbbo(
            &outbound,
            &[(Coin::new("BTC"), raw_bbo(Some(("101", "2", 9)), None))],
            3,
            (true, true, true),
            &mut cache,
        ));
        assert_eq!(
            data_rx.try_recv().expect("price delta queued").payload,
            bytes::Bytes::from_static(
                br#"{"channel":"allbbo","data":{"time":3,"bbos":[{"coin":"BTC","bid":"101","ask":null}]}}"#
            ),
        );

        assert!(send_ws_data_from_allbbo(
            &outbound,
            &[(Coin::new("BTC"), raw_bbo(Some(("101", "3", 10)), Some(("102", "1", 1))))],
            4,
            (true, true, true),
            &mut cache,
        ));
        assert_eq!(
            data_rx.try_recv().expect("side-presence delta queued").payload,
            bytes::Bytes::from_static(
                br#"{"channel":"allbbo","data":{"time":4,"bbos":[{"coin":"BTC","bid":"101","ask":"102"}]}}"#
            ),
        );
    }

    #[test]
    fn allbbo_suppressed_update_does_not_replace_heartbeat_payload() {
        let mut cache = AllBboCache::default();
        cache.prime_snapshot(
            &AllBbo {
                time: 1,
                bbos: vec![AllBboEntry { coin: "BTC".to_string(), bid: Some("100".to_string()), ask: None }],
            },
            Instant::now(),
        );
        let (outbound, mut data_rx) = outbound_for_test();

        assert!(send_ws_data_from_allbbo(
            &outbound,
            &[(Coin::new("BTC"), raw_bbo(Some(("101", "1", 1)), None))],
            2,
            (true, true, true),
            &mut cache,
        ));
        let _price_delta = data_rx.try_recv().expect("price delta queued");

        assert!(send_ws_data_from_allbbo(
            &outbound,
            &[(Coin::new("BTC"), raw_bbo(Some(("101", "4", 7)), None))],
            3,
            (true, true, true),
            &mut cache,
        ));
        assert!(matches!(data_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        let heartbeat = cache.heartbeat(Instant::now(), 4, Duration::ZERO).expect("heartbeat due");
        assert_eq!(heartbeat.time, 4);
        assert_eq!(
            heartbeat.bbos,
            vec![AllBboEntry { coin: "BTC".to_string(), bid: Some("101".to_string()), ask: None }],
        );
    }

    #[test]
    fn bbo_dedup_remains_size_sensitive() {
        let mut cache = HashMap::new();
        let mut bbos = HashMap::from([(Coin::new("BTC"), raw_bbo(Some(("100", "1", 1)), None))]);
        let (outbound, mut data_rx) = outbound_for_test();

        assert!(send_ws_data_from_bbo(&outbound, "BTC", &bbos, 1, &PayloadCache::default(), &mut cache));
        let _first = data_rx.try_recv().expect("initial bbo queued");

        bbos.insert(Coin::new("BTC"), raw_bbo(Some(("100", "2", 1)), None));
        assert!(send_ws_data_from_bbo(&outbound, "BTC", &bbos, 2, &PayloadCache::default(), &mut cache));
        let _size_delta = data_rx.try_recv().expect("size delta queued");

        bbos.insert(Coin::new("BTC"), raw_bbo(Some(("100", "2", 9)), None));
        assert!(send_ws_data_from_bbo(&outbound, "BTC", &bbos, 3, &PayloadCache::default(), &mut cache));
        assert!(matches!(data_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[test]
    fn broadcast_lag_queues_a_reconnect_error() {
        let (outbound, mut data_rx) = outbound_for_test();

        report_broadcast_lag(&outbound, 7);

        let payload = data_rx.try_recv().expect("lag error queued").payload;
        assert_eq!(
            payload,
            bytes::Bytes::from_static(
                br#"{"channel":"error","data":"Stream lagged: 7 messages were dropped; reconnect to resubscribe"}"#
            ),
        );
    }

    #[test]
    fn l4_updates_at_or_below_the_snapshot_cutoff_are_dropped() {
        let cutoffs = HashMap::from([("BTC".to_string(), L4UpdateCutoff { diffs_through: 11, statuses_through: 9 })]);
        let btc = Subscription::L4Book { coin: "BTC".to_string() };
        let eth = Subscription::L4Book { coin: "ETH".to_string() };

        assert!(l4_update_in_snapshot(&cutoffs, &btc, L4UpdateKind::Diffs, 11));
        assert!(!l4_update_in_snapshot(&cutoffs, &btc, L4UpdateKind::Diffs, 12));
        assert!(l4_update_in_snapshot(&cutoffs, &btc, L4UpdateKind::Statuses, 9));
        assert!(!l4_update_in_snapshot(&cutoffs, &btc, L4UpdateKind::Statuses, 10));
        assert!(!l4_update_in_snapshot(&cutoffs, &eth, L4UpdateKind::Diffs, 1));
        assert!(!l4_update_in_snapshot(
            &cutoffs,
            &Subscription::Trades { coin: "BTC".to_string() },
            L4UpdateKind::Diffs,
            1
        ));
    }

    fn diff_json(coin: &str, oid: u64) -> serde_json::Value {
        serde_json::json!({
            "user": "0x0000000000000000000000000000000000000000",
            "oid": oid,
            "side": "B",
            "px": "100",
            "coin": coin,
            "raw_book_diff": { "new": { "sz": "1" } }
        })
    }

    fn status_json(coin: &str, oid: u64, user: &str) -> serde_json::Value {
        serde_json::json!({
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
        })
    }

    fn fill_json(coin: &str, tid: u64) -> serde_json::Value {
        serde_json::json!([
            "0x0000000000000000000000000000000000000001",
            {
                "coin": coin,
                "px": "50000.0",
                "sz": "0.1",
                "side": "A",
                "time": 1_700_000_000_000_u64,
                "startPosition": "0",
                "dir": "Open Long",
                "closedPnl": "0",
                "hash": "0xabc",
                "oid": 123,
                "crossed": true,
                "fee": "0.5",
                "tid": tid,
                "feeToken": "USDC",
                "liquidation": null
            }
        ])
    }

    fn batch_of<E: serde::de::DeserializeOwned>(events: Vec<serde_json::Value>) -> Batch<E> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 42,
            "events": events,
        }))
        .expect("valid batch")
    }

    fn wire(msg: &ServerResponse) -> bytes::Bytes {
        bytes::Bytes::from(serde_json::to_string(msg).unwrap())
    }

    #[test]
    fn shared_book_payloads_match_per_message_serialization() {
        let diffs: Batch<NodeDataOrderDiff> =
            batch_of(vec![diff_json("BTC", 1), diff_json("ETH", 2), diff_json("BTC", 3)]);
        let btc_diffs: Vec<NodeDataOrderDiff> =
            diffs.events_ref().iter().filter(|diff| diff.coin().value() == "BTC").cloned().collect();
        let (time, height) = (diffs.block_time(), diffs.block_number());

        let mut expected_updates = L4BookUpdates::new(time, height);
        expected_updates.book_diffs.clone_from(&btc_diffs);
        assert_eq!(l4_diff_payloads(&diffs)["BTC"], wire(&ServerResponse::L4Book(L4Book::Updates(expected_updates))),);
        let raw = raw_diff_payloads(&diffs);
        assert_eq!(raw["BTC"], wire(&ServerResponse::BookDiffs(btc_diffs)));
        assert_eq!(raw.len(), 2);

        let alice = "0x00000000000000000000000000000000000000a1";
        let bob = "0x00000000000000000000000000000000000000b2";
        let statuses: Batch<NodeDataOrderStatus> =
            batch_of(vec![status_json("BTC", 1, alice), status_json("ETH", 2, bob), status_json("BTC", 3, alice)]);
        let alice: alloy::primitives::Address = alice.parse().unwrap();
        let alice_statuses: Vec<NodeDataOrderStatus> =
            statuses.events_ref().iter().filter(|status| status.user == alice).cloned().collect();
        let mut expected_updates = L4BookUpdates::new(time, height);
        expected_updates.order_statuses.clone_from(&alice_statuses);
        assert_eq!(
            l4_status_payloads(&statuses)["BTC"],
            wire(&ServerResponse::L4Book(L4Book::Updates(expected_updates))),
        );

        let expected_orders = alice_statuses
            .into_iter()
            .map(|order_status| OrderUpdate { user: alice, time, height, order_status })
            .collect();
        let order_updates = order_update_payloads(&statuses);
        assert_eq!(order_updates[&alice], wire(&ServerResponse::OrderUpdates(expected_orders)));
        assert_eq!(order_updates.len(), 2);

        let fills: Batch<NodeDataFill> = batch_of(vec![fill_json("BTC", 1), fill_json("ETH", 2), fill_json("BTC", 3)]);
        let btc_trades = fills
            .events_ref()
            .iter()
            .cloned()
            .map(Trade::from_single_fill)
            .filter(|trade| trade.coin == "BTC")
            .collect();
        assert_eq!(trade_payloads(&fills)["BTC"], wire(&ServerResponse::Trades(btc_trades)));
    }

    #[test]
    fn shared_bbo_payload_matches_per_message_serialization() {
        let raw = raw_bbo(Some(("100", "1", 2)), Some(("101", "3", 1)));
        let bbos = HashMap::from([(Coin::new("BTC"), raw)]);
        let (bid, ask) = levels_from_raw_bbo(raw);

        assert_eq!(
            bbo_payloads(&bbos, 7)["BTC"],
            wire(&ServerResponse::Bbo(Bbo { coin: "BTC".to_string(), time: 7, bid, ask })),
        );
    }

    #[test]
    fn subscribers_of_one_message_share_its_payload_bytes() {
        let bbos = HashMap::from([(Coin::new("BTC"), raw_bbo(Some(("100", "1", 1)), None))]);
        let payloads = PayloadCache::default();
        let (first, mut first_rx) = outbound_for_test();
        let (second, mut second_rx) = outbound_for_test();

        assert!(send_ws_data_from_bbo(&first, "BTC", &bbos, 1, &payloads, &mut HashMap::new()));
        assert!(send_ws_data_from_bbo(&second, "BTC", &bbos, 1, &payloads, &mut HashMap::new()));

        let first = first_rx.try_recv().expect("first queued").payload;
        let second = second_rx.try_recv().expect("second queued").payload;
        assert_eq!(first.as_ptr(), second.as_ptr(), "one serialization, shared");
    }

    #[test]
    fn pong_frame_matches_server_response_serialization() {
        assert_eq!(PONG_FRAME, serde_json::to_string(&ServerResponse::Pong).unwrap().as_bytes());
    }

    #[tokio::test]
    async fn write_task_sends_pongs_before_queued_data() {
        let (data_tx, data_rx) = mpsc::channel(8);
        let (pong_tx, pong_rx) = mpsc::channel(8);
        let outbound = Outbound::new(data_tx, pong_tx);
        for i in 0..3 {
            assert!(outbound.send_payload(bytes::Bytes::from(format!("data{i}"))));
        }
        assert!(outbound.send_pong());
        drop(outbound);

        let sink = RecordingSink::default();
        write_task(sink.clone(), data_rx, pong_rx).await;

        let frames = sink.0.lock().unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].as_ref(), PONG_FRAME);
    }

    #[tokio::test]
    async fn outbound_fails_when_writer_is_gone() {
        let (data_tx, data_rx) = mpsc::channel(8);
        let (pong_tx, pong_rx) = mpsc::channel(8);
        drop((data_rx, pong_rx));

        let outbound = Outbound::new(data_tx, pong_tx);

        assert!(!outbound.send_payload(bytes::Bytes::from_static(b"x")));
        assert!(!outbound.send_pong());
        outbound.data_tx.closed().await;
        drop(outbound);
    }

    #[tokio::test]
    async fn full_pong_lane_drops_connection() {
        let (data_tx, _data_rx) = mpsc::channel(8);
        let (pong_tx, _pong_rx) = mpsc::channel(2);
        let outbound = Outbound::new(data_tx, pong_tx);

        assert!(outbound.send_pong());
        assert!(outbound.send_pong());
        assert!(!outbound.send_pong());
        drop(outbound);
    }

    #[tokio::test(start_paused = true)]
    async fn full_data_lane_drops_connection_without_waiting() {
        let (data_tx, _data_rx) = mpsc::channel(1);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        let outbound = Outbound::new(data_tx, pong_tx);

        assert!(outbound.send_payload(bytes::Bytes::from_static(b"first")));
        let started = tokio::time::Instant::now();
        assert!(!outbound.send_payload(bytes::Bytes::from_static(b"second")));
        assert_eq!(started.elapsed(), Duration::ZERO);
        drop(outbound);
    }

    #[test]
    fn oversized_frame_uses_the_whole_byte_budget() {
        let (data_tx, _data_rx) = mpsc::channel(2);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        let outbound = Outbound::new(data_tx, pong_tx);

        assert!(outbound.send_payload(bytes::Bytes::from(vec![0; OUTBOUND_BYTE_BUDGET + 1])));
        assert!(!outbound.send_payload(bytes::Bytes::from_static(b"blocked")));
        drop(outbound);
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_writer_times_out_and_closes_queue() {
        let (data_tx, data_rx) = mpsc::channel(1);
        let (pong_tx, pong_rx) = mpsc::channel(1);
        let outbound = Outbound::new(data_tx, pong_tx);
        let writer = tokio::spawn(write_task(StuckSink, data_rx, pong_rx));

        assert!(outbound.send_payload(bytes::Bytes::from_static(b"x")));
        writer.await.unwrap();
        assert!(!outbound.send_payload(bytes::Bytes::from_static(b"y")));
        drop(outbound);
    }
}

fn serialize_grouped<K: Eq + std::hash::Hash, V>(
    groups: HashMap<K, V>,
    wrap: impl Fn(V) -> ServerResponse,
) -> HashMap<K, bytes::Bytes> {
    groups.into_iter().filter_map(|(key, value)| Some((key, serialize_response(&wrap(value))?))).collect()
}

/// Trades per coin, serialized.
fn trade_payloads(batch: &Batch<NodeDataFill>) -> HashMap<String, bytes::Bytes> {
    let mut trades: HashMap<String, Vec<Trade>> = HashMap::new();
    // Convert each fill directly to a trade (no pairing)
    for fill in batch.events_ref() {
        let trade = Trade::from_single_fill(fill.clone());
        trades.entry(trade.coin.clone()).or_default().push(trade);
    }
    serialize_grouped(trades, ServerResponse::Trades)
}

/// L4 book updates per coin from an order diffs batch (without statuses), serialized.
fn l4_diff_payloads(batch: &Batch<NodeDataOrderDiff>) -> HashMap<String, bytes::Bytes> {
    let (time, height) = (batch.block_time(), batch.block_number());
    let mut updates: HashMap<String, L4BookUpdates> = HashMap::new();
    for diff in batch.events_ref() {
        updates
            .entry(diff.coin().value())
            .or_insert_with(|| L4BookUpdates::new(time, height))
            .book_diffs
            .push(diff.clone());
    }
    serialize_grouped(updates, |updates| ServerResponse::L4Book(L4Book::Updates(updates)))
}

/// L4 book updates per coin from an order statuses batch (without diffs), serialized.
fn l4_status_payloads(batch: &Batch<NodeDataOrderStatus>) -> HashMap<String, bytes::Bytes> {
    let (time, height) = (batch.block_time(), batch.block_number());
    let mut updates: HashMap<String, L4BookUpdates> = HashMap::new();
    for status in batch.events_ref() {
        updates
            .entry(status.order.coin.clone())
            .or_insert_with(|| L4BookUpdates::new(time, height))
            .order_statuses
            .push(status.clone());
    }
    serialize_grouped(updates, |updates| ServerResponse::L4Book(L4Book::Updates(updates)))
}

/// Raw book diffs per coin, serialized.
fn raw_diff_payloads(batch: &Batch<NodeDataOrderDiff>) -> HashMap<String, bytes::Bytes> {
    let mut grouped: HashMap<String, Vec<NodeDataOrderDiff>> = HashMap::new();
    for diff in batch.events_ref() {
        grouped.entry(diff.coin().value()).or_default().push(diff.clone());
    }
    serialize_grouped(grouped, ServerResponse::BookDiffs)
}

/// Order updates per user, serialized.
fn order_update_payloads(batch: &Batch<NodeDataOrderStatus>) -> HashMap<alloy::primitives::Address, bytes::Bytes> {
    let (time, height) = (batch.block_time(), batch.block_number());
    let mut by_user: HashMap<alloy::primitives::Address, Vec<OrderUpdate>> = HashMap::new();
    for status in batch.events_ref() {
        by_user.entry(status.user).or_default().push(OrderUpdate {
            user: status.user,
            time,
            height,
            order_status: status.clone(),
        });
    }
    serialize_grouped(by_user, ServerResponse::OrderUpdates)
}

/// Send `coin`'s payload from a per-coin map, if the message has one.
fn send_coin_payload(
    outbound: &Outbound,
    payloads: &HashMap<String, bytes::Bytes>,
    coin: &str,
    broadcasts: &IntCounter,
) -> bool {
    let Some(payload) = payloads.get(coin) else { return true };
    broadcasts.inc();
    outbound.send_payload(payload.clone())
}

fn send_ws_stats(outbound: &Outbound, stats: Stats) -> bool {
    BROADCASTS.stats.inc();
    outbound.send_message(ServerResponse::Stats(stats))
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: &ListenerClient,
        market_filter: (bool, bool, bool),
        l4_cutoffs: &mut HashMap<String, L4UpdateCutoff>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let requested_coin = Coin::new(coin);
            if let Some(coin_snapshot) = listener.l4_snapshot(requested_coin.clone()).await? {
                l4_cutoffs.insert(coin.clone(), L4UpdateCutoff::from_snapshot(&coin_snapshot));
                let L4CoinSnapshot { time, height, snapshot, .. } = coin_snapshot;
                let levels = snapshot.into_inner().map(|orders| orders.into_iter().map(L4Order::from).collect());
                return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                    coin: requested_coin.value(),
                    time,
                    height,
                    levels,
                })));
            }
            return Err("Snapshot Failed".into());
        }
        if matches!(self, Self::AllBbo) {
            if let Some((time, bbos)) = listener.all_bbos().await? {
                let bbos = bbos
                    .into_iter()
                    .filter(|(coin, _)| market_filter_allows_coin_ref(coin, market_filter))
                    .map(|(coin, raw_bbo)| allbbo_entry_from_raw(&coin, raw_bbo))
                    .collect();
                return Ok(Some(ServerResponse::AllBbo(AllBbo { time, bbos })));
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

/// Send order updates to OrderUpdates subscribers filtered by user address
fn send_ws_order_updates(
    outbound: &Outbound,
    subscription: &Subscription,
    payloads: &HashMap<alloy::primitives::Address, bytes::Bytes>,
) -> bool {
    let Subscription::OrderUpdates { user } = subscription else { return true };
    // Invalid address, skip (validation should already prevent this)
    let Ok(user) = user.parse::<alloy::primitives::Address>() else { return true };
    payloads.get(&user).is_none_or(|payload| outbound.send_payload(payload.clone()))
}
