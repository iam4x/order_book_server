use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{Router, extract::Query, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    select,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

use crate::{
    FeatureSet, ServerConfig,
    listeners::order_book::{
        AllBboSubscriptionRegistry, InternalMessage, L2SubscriptionKey, L2SubscriptionRegistry, OrderBookListener,
        PreparedL2Book, TimedSnapshots, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, RawBbo},
    prelude::*,
    types::{
        AllBbo, AllBboEntry, Bbo, L2Book, L4Book, L4BookUpdates, L4Order, Level, Trade,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, OrderUpdate, ServerResponse, Subscription, SubscriptionManager},
    },
};

#[derive(Debug, Deserialize)]
struct WsAuthQuery {
    token: Option<String>,
}

/// Per-subscription cached L2 broadcast. `version` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires.
struct L2Entry {
    version: u64,
    last_sent: Instant,
    payload: L2Book,
}

/// Per-coin cached BBO broadcast. `tuple` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires.
struct BboEntry {
    tuple: BboDedupTuple,
    last_sent: Instant,
    payload: Bbo,
}

#[derive(Clone, Eq, PartialEq)]
struct BboDedupTuple {
    bid_px: String,
    bid_sz: String,
    ask_px: String,
    ask_sz: String,
}

#[derive(Default)]
struct AllBboCache {
    tuples: HashMap<String, BboDedupTuple>,
    last_sent: Option<Instant>,
    payload: Option<AllBbo>,
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
    // Broadcast channel buffer. Each buffered Snapshot now holds Arc'd inner maps
    // shared across receivers, so deep cloning is no longer the cost - but a slow
    // receiver still pins one Arc<InternalMessage> per buffered slot. 32 is well
    // above the steady-state queue depth and keeps worst-case transient memory bounded.
    // Slow receivers fall into the existing `RecvError::Lagged` shedding path
    // (CHANNEL_DROPS_TOTAL is incremented).
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(32);

    // Market filter flags from config
    let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
    let ignore_spot = !config.include_spot; // For OrderBookListener (legacy)
    let compression_level = config.compression_level;

    // Resolve data directory
    // Central task: listen to messages and forward them for distribution
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot, config.features)
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        let config = config.clone();
        tokio::spawn(async move {
            info!("Starting HFT-optimized listener");
            let result = hl_listen_hft(listener, config).await;
            if let Err(err) = result {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let websocket_secret = config.secret.as_deref().map(Arc::<str>::from);

    let start_time = Instant::now();
    let listener_for_health = listener.clone();
    let l2_subscription_registry = listener.lock().await.l2_subscription_registry();
    let allbbo_subscription_registry = listener.lock().await.allbbo_subscription_registry();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let l2book_heartbeat_ms = config.l2book_heartbeat_ms;
                let bbo_heartbeat_ms = config.bbo_heartbeat_ms;
                let allbbo_heartbeat_ms = config.allbbo_heartbeat_ms;
                let features = config.features;
                let listener = listener.clone();
                let l2_subscription_registry = Arc::clone(&l2_subscription_registry);
                let allbbo_subscription_registry = Arc::clone(&allbbo_subscription_registry);
                let websocket_secret = websocket_secret.clone();
                move |Query(query): Query<WsAuthQuery>, ws_upgrade| {
                    let websocket_secret = websocket_secret.clone();
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
                    let is_ready = listener.lock().await.is_ready();
                    let uptime_secs = start_time.elapsed().as_secs();
                    let height = ORDERBOOK_HEIGHT.get();
                    let connections = WS_CONNECTIONS_ACTIVE.get();
                    let body = format!(
                        r#"{{"status":"{}","uptime_seconds":{},"height":{},"connections":{}}}"
                    "#,
                        if is_ready { "ready" } else { "initializing" },
                        uptime_secs,
                        height,
                        connections,
                    );
                    axum::response::Response::builder().header("content-type", "application/json").body(body).unwrap()
                }
            }),
        );

    let tcp_listener = TcpListener::bind(&config.address).await?;
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
    listener: Arc<Mutex<OrderBookListener>>,
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    allbbo_subscription_registry: Arc<AllBboSubscriptionRegistry>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    features: FeatureSet,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    allbbo_heartbeat_ms: u64,
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
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    allbbo_subscription_registry: Arc<AllBboSubscriptionRegistry>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    features: FeatureSet,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    allbbo_heartbeat_ms: u64,
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
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe =
        filter_universe(&listener.lock().await.universe(), market_filter.0, market_filter.1, market_filter.2);
    // Per-subscription cache for L2 dedup + heartbeat resend.
    let mut last_l2: HashMap<L2SubscriptionKey, L2Entry> = HashMap::new();
    // Per-coin cache for BBO dedup + heartbeat resend
    let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
    let mut last_allbbo = AllBboCache::default();
    let mut l2_registrations = ConnectionL2Registrations::new(l2_subscription_registry);
    let mut allbbo_registration = ConnectionAllBboRegistration::new(allbbo_subscription_registry);
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        let _ = send_socket_message(&mut socket, msg).await;
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

    // `alive` flips to false the moment any `send_socket_message` returns false
    // (network error or send timeout). The outer loop checks it at every iteration
    // boundary so a wedged client is dropped instead of looping forever.
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
                                        alive &= send_ws_data_from_l2_update(&mut socket, sub, l2_books, &mut last_l2).await;
                                    }
                                }
                            },
                            InternalMessage::Universe{ universe: next_universe } => {
                                universe = filter_universe(next_universe, market_filter.0, market_filter.1, market_filter.2);
                            },
                            InternalMessage::BboUpdate{ bbos, time } => {
                                if !features.bbo() {
                                    continue;
                                }
                                // Fast path for BBO subscribers only
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    if let Subscription::Bbo { coin } = sub {
                                        alive &= send_ws_data_from_bbo(&mut socket, coin, bbos, *time, &mut last_bbo).await;
                                    }
                                }
                            },
                            InternalMessage::AllBboUpdate{ bbos, time } => {
                                if !features.allbbo() || !manager.subscriptions().contains(&Subscription::AllBbo) {
                                    continue;
                                }
                                alive &= send_ws_data_from_allbbo(
                                    &mut socket,
                                    bbos,
                                    *time,
                                    market_filter,
                                    &mut last_allbbo,
                                ).await;
                            },
                            InternalMessage::Fills{ batch } => {
                                if !features.trades() {
                                    continue;
                                }
                                let has_trades = manager.subscriptions().iter().any(|s| matches!(s, Subscription::Trades { .. }));
                                if has_trades {
                                    let mut trades = coin_to_trades(batch);
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        alive &= send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                    }
                                }
                            },
                            InternalMessage::L4OrderDiffs{ batch } => {
                                if !features.l4book() && !features.bookdiffs() {
                                    continue;
                                }
                                let has_l4 = manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. }));
                                let has_book_diffs = manager.subscriptions().iter().any(|s| matches!(s, Subscription::BookDiffs { .. }));
                                if has_l4 || has_book_diffs {
                                    let mut book_updates = if has_l4 { Some(coin_to_book_diffs_only(batch)) } else { None };
                                    let mut raw_diffs = if has_book_diffs { Some(coin_to_book_diffs_raw(batch)) } else { None };
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        if let Some(ref mut updates) = book_updates {
                                            alive &= send_ws_data_from_book_updates(&mut socket, sub, updates).await;
                                        }
                                        if !alive { break; }
                                        if let Some(ref mut diffs) = raw_diffs {
                                            alive &= send_ws_data_from_book_diffs_raw(&mut socket, sub, diffs).await;
                                        }
                                    }
                                }
                            },
                            InternalMessage::L4OrderStatuses{ batch } => {
                                if !features.l4book() && !features.orderupdates() {
                                    continue;
                                }
                                let has_l4 = manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. }));
                                let has_order_updates = manager.subscriptions().iter().any(|s| matches!(s, Subscription::OrderUpdates { .. }));
                                if has_l4 {
                                    let mut book_updates = coin_to_book_statuses_only(batch);
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        alive &= send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                    }
                                }
                                if has_order_updates {
                                    for sub in manager.subscriptions() {
                                        if !alive { break; }
                                        alive &= send_ws_order_updates(&mut socket, sub, batch).await;
                                    }
                                }
                            },
                        }

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        CHANNEL_LAG.set(n as i64);
                        CHANNEL_DROPS_TOTAL.inc();
                        log::debug!("Receiver lagged: {n} messages");
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
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
                            if let Some(entry) = last_l2.get_mut(&key) {
                                if now.duration_since(entry.last_sent) >= hb {
                                    entry.payload.set_time(now_ms);
                                    entry.last_sent = now;
                                    BROADCASTS_TOTAL.with_label_values(&["l2_heartbeat"]).inc();
                                    let payload = entry.payload.clone();
                                    alive &= send_socket_message(&mut socket, ServerResponse::L2Book(payload)).await;
                                }
                            }
                        }
                        Subscription::Bbo { coin } => {
                            let Some(hb) = bbo_hb else { continue };
                            if let Some(entry) = last_bbo.get_mut(coin) {
                                if now.duration_since(entry.last_sent) >= hb {
                                    entry.payload.time = now_ms;
                                    entry.last_sent = now;
                                    BROADCASTS_TOTAL.with_label_values(&["bbo_heartbeat"]).inc();
                                    let payload = entry.payload.clone();
                                    alive &= send_socket_message(&mut socket, ServerResponse::Bbo(payload)).await;
                                }
                            }
                        }
                        Subscription::AllBbo => {
                            let Some(hb) = allbbo_hb else { continue };
                            let Some(last_sent) = last_allbbo.last_sent else { continue };
                            if now.duration_since(last_sent) >= hb {
                                if let Some(payload) = last_allbbo.payload.as_mut() {
                                    payload.time = now_ms;
                                    last_allbbo.last_sent = Some(now);
                                    BROADCASTS_TOTAL.with_label_values(&["allbbo_heartbeat"]).inc();
                                    alive &= send_socket_message(
                                        &mut socket,
                                        ServerResponse::AllBbo(payload.clone()),
                                    ).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::Ping => {
                                        alive &= send_socket_message(&mut socket, ServerResponse::Pong).await;
                                    }
                                    _ => {
                                        alive &= receive_client_message(
                                            &mut socket,
                                            &mut manager,
                                            value,
                                            &universe,
                                            listener.clone(),
                                            features,
                                            market_filter,
                                            &mut last_l2,
                                            &mut last_bbo,
                                            &mut last_allbbo,
                                            &mut l2_registrations,
                                            &mut allbbo_registration,
                                        ).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                alive &= send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
    info!("Dropping connection: socket write failed or timed out");
}

#[allow(clippy::too_many_arguments)]
async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    features: FeatureSet,
    market_filter: (bool, bool, bool),
    last_l2: &mut HashMap<L2SubscriptionKey, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    last_allbbo: &mut AllBboCache,
    l2_registrations: &mut ConnectionL2Registrations,
    allbbo_registration: &mut ConnectionAllBboRegistration,
) -> bool {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    if !subscription_feature_enabled(&subscription, features) {
        return send_socket_message(
            socket,
            ServerResponse::Error(format!("Feature disabled for subscription type: {}", subscription.type_label())),
        )
        .await;
    }
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !validate_subscription_for_features(&subscription, universe, features, market_filter) {
        return send_socket_message(socket, ServerResponse::Error(format!("Invalid subscription: {sub}"))).await;
    }

    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => match manager.subscribe(subscription.clone()) {
            Ok(inserted) => ("", inserted),
            Err(err) => {
                return send_socket_message(socket, ServerResponse::Error(format!("Rejected subscription: {err}")))
                    .await;
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
                    _ => {}
                }
            }
            ("un", removed)
        }
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        if let ClientMessage::Subscribe { subscription } = &client_message {
            l2_registrations.register(subscription);
            allbbo_registration.register(subscription);
        }
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener, market_filter).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    l2_registrations.unregister(subscription);
                    allbbo_registration.unregister(subscription);
                    return send_socket_message(
                        socket,
                        ServerResponse::Error(format!("Unable to grab order book snapshot: {err}")),
                    )
                    .await;
                }
            }
        } else {
            None
        };
        if !send_socket_message(socket, ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        if let Some(snapshot_msg) = snapshot_msg {
            if let ServerResponse::AllBbo(payload) = &snapshot_msg {
                prime_allbbo_cache(last_allbbo, payload);
            }
            return send_socket_message(socket, snapshot_msg).await;
        }
        true
    } else {
        send_socket_message(socket, ServerResponse::Error(format!("Already {word}subscribed: {sub}"))).await
    }
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation.
/// Returns false if the socket send failed/timed out (caller must drop the connection).
async fn send_ws_data_from_bbo(
    socket: &mut WebSocket,
    coin: &str,
    bbos: &HashMap<Coin, RawBbo>,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
) -> bool {
    let coin_key = Coin::new(coin);
    if let Some(raw_bbo) = bbos.get(&coin_key).copied() {
        let (bid, ask) = levels_from_raw_bbo(raw_bbo);
        let current = dedup_tuple_from_levels(bid.as_ref(), ask.as_ref());

        if last_bbo.get(coin).map(|e| &e.tuple) != Some(&current) {
            BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
            BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
            let bbo = Bbo { coin: coin.to_string(), time, bid, ask };
            last_bbo
                .insert(coin.to_string(), BboEntry { tuple: current, last_sent: Instant::now(), payload: bbo.clone() });
            return send_socket_message(socket, ServerResponse::Bbo(bbo)).await;
        }
    }
    true
}

async fn send_ws_data_from_allbbo(
    socket: &mut WebSocket,
    bbos: &[(Coin, RawBbo)],
    time: u64,
    market_filter: (bool, bool, bool),
    last_allbbo: &mut AllBboCache,
) -> bool {
    let mut changed = Vec::with_capacity(bbos.len());
    for (coin, raw_bbo) in bbos {
        if !market_filter_allows_coin_ref(coin, market_filter) {
            continue;
        }
        let entry = allbbo_entry_from_raw(coin, *raw_bbo);
        let current = dedup_tuple_from_levels(entry.bid.as_ref(), entry.ask.as_ref());
        if last_allbbo.tuples.get(&entry.coin) == Some(&current) {
            continue;
        }
        last_allbbo.tuples.insert(entry.coin.clone(), current);
        changed.push(entry);
    }

    if changed.is_empty() {
        return true;
    }

    BROADCASTS_TOTAL.with_label_values(&["allbbo"]).inc();
    let payload = AllBbo { time, bbos: changed };
    last_allbbo.last_sent = Some(Instant::now());
    last_allbbo.payload = Some(payload.clone());
    send_socket_message(socket, ServerResponse::AllBbo(payload)).await
}

fn level_from_raw(raw: Option<(crate::order_book::Px, crate::order_book::Sz, u32)>) -> Option<Level> {
    raw.map(|(px, sz, n)| Level::new(px.to_str(), sz.to_str(), n as usize))
}

fn levels_from_raw_bbo(raw_bbo: RawBbo) -> (Option<Level>, Option<Level>) {
    (level_from_raw(raw_bbo.0), level_from_raw(raw_bbo.1))
}

fn allbbo_entry_from_raw(coin: &Coin, raw_bbo: RawBbo) -> AllBboEntry {
    let (bid, ask) = levels_from_raw_bbo(raw_bbo);
    AllBboEntry { coin: coin.value(), bid, ask }
}

fn dedup_tuple_from_levels(bid: Option<&Level>, ask: Option<&Level>) -> BboDedupTuple {
    BboDedupTuple {
        bid_px: bid.map(Level::px).unwrap_or_default().to_string(),
        bid_sz: bid.map(Level::sz).unwrap_or_default().to_string(),
        ask_px: ask.map(Level::px).unwrap_or_default().to_string(),
        ask_sz: ask.map(Level::sz).unwrap_or_default().to_string(),
    }
}

fn prime_allbbo_cache(cache: &mut AllBboCache, payload: &AllBbo) {
    cache.tuples.clear();
    for entry in &payload.bbos {
        cache.tuples.insert(entry.coin.clone(), dedup_tuple_from_levels(entry.bid.as_ref(), entry.ask.as_ref()));
    }
    cache.last_sent = Some(Instant::now());
    cache.payload = Some(payload.clone());
}

/// Per-send timeout. A slow or hostile client whose TCP receive window stays full
/// would otherwise block `socket.send(...).await` indefinitely, freezing this
/// connection's whole `select!` loop and accumulating broadcast lag.
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Send a `ServerResponse` to the client. Returns `false` when the underlying
/// socket failed to write (network error or `WS_SEND_TIMEOUT` elapsed). Callers
/// in the `select!` loop must bail out on `false` so we drop the doomed
/// connection instead of looping forever on a wedged write.
async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) -> bool {
    let payload = match serde_json::to_string(&msg) {
        Ok(p) => p,
        Err(err) => {
            error!("Server response serialization error: {err}");
            // Serialization failure is our bug, not the client's; keep the connection.
            return true;
        }
    };
    match tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(FrameView::text(payload))).await {
        Ok(Ok(())) => {
            MESSAGES_SENT_TOTAL.inc();
            true
        }
        Ok(Err(err)) => {
            error!("Failed to send: {err}");
            WS_SEND_ERRORS_TOTAL.inc();
            false
        }
        Err(_) => {
            error!("Send timeout (>{:?}); dropping slow client", WS_SEND_TIMEOUT);
            WS_SEND_ERRORS_TOTAL.inc();
            // Best-effort close handshake. If the close itself times out we just drop.
            let _unused = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
            false
        }
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
        .filter_map(|c| {
            let include =
                (c.is_perp() && include_perps) || (c.is_spot() && include_spot) || (c.is_hip3() && include_hip3);
            if include { Some(c.clone().value()) } else { None }
        })
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
        Subscription::AllBbo => true,
        Subscription::Trades { coin } | Subscription::BookDiffs { coin } => {
            market_filter_allows_coin(coin, market_filter)
        }
        Subscription::OrderUpdates { .. } => subscription.validate(universe),
        Subscription::Bbo { .. } | Subscription::L2Book { .. } | Subscription::L4Book { .. } => false,
    }
}

async fn send_ws_data_from_l2_update(
    socket: &mut WebSocket,
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

    if last_l2.get(&key).map(|entry| entry.version) == Some(prepared.version()) {
        return true;
    }

    BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
    let payload = prepared.payload().clone();
    last_l2.insert(key, L2Entry { version: prepared.version(), last_sent: Instant::now(), payload: payload.clone() });
    send_socket_message(socket, ServerResponse::L2Book(payload)).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
        registrations.unregister(&default_levels);

        assert!(!registry.active_keys().contains(&default_key));
        assert!(registry.active_keys().contains(&ten_levels_key));
        assert!(registry.active_coins().contains(&Coin::new("BTC")));

        registrations.unregister(&ten_levels);

        assert!(registry.active_keys().is_empty());
        assert!(registry.active_coins().is_empty());
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
    fn allbbo_cache_suppresses_unchanged_entries() {
        let mut cache = AllBboCache::default();
        let first = AllBbo {
            time: 1,
            bbos: vec![AllBboEntry {
                coin: "BTC".to_string(),
                bid: Some(Level::new("100".to_string(), "1".to_string(), 1)),
                ask: None,
            }],
        };
        prime_allbbo_cache(&mut cache, &first);

        let raw = vec![(
            Coin::new("BTC"),
            (
                Some((
                    crate::order_book::Px::parse_from_str("100").expect("valid px"),
                    crate::order_book::Sz::parse_from_str("1").expect("valid sz"),
                    1,
                )),
                None,
            ),
        )];
        let mut changed = Vec::new();
        for (coin, raw_bbo) in &raw {
            let entry = allbbo_entry_from_raw(coin, *raw_bbo);
            let current = dedup_tuple_from_levels(entry.bid.as_ref(), entry.ask.as_ref());
            if cache.tuples.get(&entry.coin) != Some(&current) {
                changed.push(entry);
            }
        }

        assert!(changed.is_empty());
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let fills = batch.clone().events();
    let mut trades = HashMap::new();

    // Convert each fill directly to a trade (no pairing)
    for fill in fills {
        let trade = Trade::from_single_fill(fill);
        let coin = trade.coin.clone();
        trades.entry(coin).or_insert_with(Vec::new).push(trade);
    }

    trades
}

/// HFT helper: convert order diffs batch to book updates (without statuses)
fn coin_to_book_diffs_only(diff_batch: &Batch<NodeDataOrderDiff>) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    updates
}

/// HFT helper: convert order statuses batch to book updates (without diffs)
fn coin_to_book_statuses_only(status_batch: &Batch<NodeDataOrderStatus>) -> HashMap<String, L4BookUpdates> {
    let statuses = status_batch.clone().events();
    let time = status_batch.block_time();
    let height = status_batch.block_number();
    let mut updates = HashMap::new();
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

fn coin_to_book_diffs_raw(batch: &Batch<NodeDataOrderDiff>) -> HashMap<String, Vec<NodeDataOrderDiff>> {
    let diffs = batch.clone().events();
    let mut grouped = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        grouped.entry(coin).or_insert_with(Vec::new).push(diff);
    }
    grouped
}

async fn send_ws_data_from_book_diffs_raw(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_diffs: &mut HashMap<String, Vec<NodeDataOrderDiff>>,
) -> bool {
    if let Subscription::BookDiffs { coin } = subscription {
        if let Some(diffs) = book_diffs.remove(coin) {
            BROADCASTS_TOTAL.with_label_values(&["bookDiffs"]).inc();
            return send_socket_message(socket, ServerResponse::BookDiffs(diffs)).await;
        }
    }
    true
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) -> bool {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
            return send_socket_message(socket, ServerResponse::L4Book(L4Book::Updates(updates))).await;
        }
    }
    true
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) -> bool {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
            return send_socket_message(socket, ServerResponse::Trades(trades)).await;
        }
    }
    true
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
        market_filter: (bool, bool, bool),
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let requested_coin = Coin::new(coin);
                let filtered =
                    snapshot.value().into_iter().filter(|(c, _)| *c == requested_coin).collect::<Vec<_>>().pop();
                if let Some((found_coin, coin_snapshot)) = filtered {
                    let levels =
                        coin_snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: found_coin.value(),
                        time,
                        height,
                        levels,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        if matches!(self, Self::AllBbo) {
            if let Some((time, bbos)) = listener.lock().await.all_bbos() {
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
async fn send_ws_order_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    batch: &Batch<NodeDataOrderStatus>,
) -> bool {
    if let Subscription::OrderUpdates { user } = subscription {
        // Parse the user address from the subscription
        let user_addr = match user.parse::<alloy::primitives::Address>() {
            Ok(addr) => addr,
            Err(_) => return true, // Invalid address, skip (validation should already prevent this)
        };

        let time = batch.block_time();
        let height = batch.block_number();
        let statuses = batch.clone().events();

        // Filter statuses for this specific user
        let user_updates: Vec<OrderUpdate> = statuses
            .into_iter()
            .filter(|status| status.user == user_addr)
            .map(|status| OrderUpdate::new(status.user, time, height, status))
            .collect();

        if !user_updates.is_empty() {
            return send_socket_message(socket, ServerResponse::OrderUpdates(user_updates)).await;
        }
    }
    true
}
