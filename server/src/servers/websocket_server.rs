use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2SubscriptionRegistry, OrderBookListener, TimedSnapshots, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, Px, Snapshot, Sz},
    prelude::*,
    types::{
        Bbo, L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, OrderUpdate, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

use crate::ServerConfig;

/// Per-(coin, params) cached L2 broadcast. `hash` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires.
struct L2Entry {
    hash: u64,
    last_sent: Instant,
    payload: L2Book,
}

/// Per-coin cached BBO broadcast. `tuple` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires.
struct BboEntry {
    tuple: (String, String, String, String),
    last_sent: Instant,
    payload: Bbo,
}

fn l2_cache_key(coin: &str, n_sig_figs: Option<u32>, mantissa: Option<u64>) -> String {
    format!("{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0))
}

struct ConnectionL2Registrations {
    registry: Arc<L2SubscriptionRegistry>,
    subscriptions: HashSet<(Coin, L2SnapshotParams, Option<usize>)>,
}

impl ConnectionL2Registrations {
    fn new(registry: Arc<L2SubscriptionRegistry>) -> Self {
        Self { registry, subscriptions: HashSet::new() }
    }

    fn register(&mut self, subscription: &Subscription) {
        if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
            let coin = Coin::new(coin);
            let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
            if self.subscriptions.insert((coin.clone(), params, *n_levels)) {
                self.registry.register_l2(coin, params);
            }
        }
    }

    fn unregister(&mut self, subscription: &Subscription) {
        if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
            let coin = Coin::new(coin);
            let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
            if self.subscriptions.remove(&(coin.clone(), params, *n_levels)) {
                self.registry.unregister_l2(&coin, params);
            }
        }
    }
}

impl Drop for ConnectionL2Registrations {
    fn drop(&mut self) {
        for (coin, params, _) in self.subscriptions.drain() {
            self.registry.unregister_l2(&coin, params);
        }
    }
}

/// Build a tokio interval that fires often enough to drive both heartbeats with
/// at most half the configured period of drift. Returns None when both heartbeats are disabled.
fn build_heartbeat_ticker(l2book_heartbeat_ms: u64, bbo_heartbeat_ms: u64) -> Option<tokio::time::Interval> {
    let enabled = [l2book_heartbeat_ms, bbo_heartbeat_ms].into_iter().filter(|&ms| ms > 0).min()?;
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
        OrderBookListener::new(Some(internal_message_tx), ignore_spot)
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

    let start_time = Instant::now();
    let listener_for_health = listener.clone();
    let l2_subscription_registry = listener.lock().await.l2_subscription_registry();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let bbo_only = config.bbo_only;
                let l2book_heartbeat_ms = config.l2book_heartbeat_ms;
                let bbo_heartbeat_ms = config.bbo_heartbeat_ms;
                let listener = listener.clone();
                let l2_subscription_registry = Arc::clone(&l2_subscription_registry);
                move |ws_upgrade| async move {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        Arc::clone(&l2_subscription_registry),
                        market_filter,
                        bbo_only,
                        l2book_heartbeat_ms,
                        bbo_heartbeat_ms,
                        websocket_opts,
                    )
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
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    websocket_opts: yawc::Options,
) -> axum::response::Response {
    use axum::response::IntoResponse;
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
            market_filter,
            bbo_only,
            l2book_heartbeat_ms,
            bbo_heartbeat_ms,
        )
        .await
    });

    resp.into_response()
}

#[allow(clippy::too_many_arguments)]
async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    l2_subscription_registry: Arc<L2SubscriptionRegistry>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
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
    let mut universe = filter_universe(&listener.lock().await.universe(), market_filter.0, market_filter.1, market_filter.2);
    // Per-(coin,params) cache for L2 dedup + heartbeat resend (key = "<coin>:<n_sig_figs>:<mantissa>")
    let mut last_l2: HashMap<String, L2Entry> = HashMap::new();
    // Per-coin cache for BBO dedup + heartbeat resend
    let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
    let mut l2_registrations = ConnectionL2Registrations::new(l2_subscription_registry);
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        let _ = send_socket_message(&mut socket, msg).await;
        return;
    }

    // Optional heartbeat ticker. We tick at min(enabled_heartbeats)/2 (clamped to [50, 500] ms)
    // so each subscription's last-sent timestamp can drift at most half a heartbeat from the configured value.
    let mut heartbeat_ticker = build_heartbeat_ticker(l2book_heartbeat_ms, bbo_heartbeat_ms);
    let l2_hb = if l2book_heartbeat_ms > 0 { Some(Duration::from_millis(l2book_heartbeat_ms)) } else { None };
    let bbo_hb = if bbo_heartbeat_ms > 0 { Some(Duration::from_millis(bbo_heartbeat_ms)) } else { None };

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
                            InternalMessage::L2Update{ l2_snapshots, time } => {
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    // Partial L2 updates intentionally omit unchanged coins.
                                    if !matches!(sub, Subscription::Bbo { .. }) {
                                        alive &= send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, &mut last_bbo, &mut last_l2, false).await;
                                    }
                                }
                            },
                            InternalMessage::Universe{ universe: next_universe } => {
                                universe = filter_universe(next_universe, market_filter.0, market_filter.1, market_filter.2);
                            },
                            InternalMessage::BboUpdate{ bbos, time } => {
                                // Fast path for BBO subscribers only
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    if let Subscription::Bbo { coin } = sub {
                                        alive &= send_ws_data_from_bbo(&mut socket, coin, bbos, *time, &mut last_bbo).await;
                                    }
                                }
                            },
                            InternalMessage::Fills{ batch } => {
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
                        Subscription::L2Book { coin, n_sig_figs, mantissa, .. } => {
                            let Some(hb) = l2_hb else { continue };
                            let key = l2_cache_key(coin, *n_sig_figs, *mantissa);
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
                                            bbo_only,
                                            &mut last_l2,
                                            &mut last_bbo,
                                            &mut l2_registrations,
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
    bbo_only: bool,
    last_l2: &mut HashMap<String, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    l2_registrations: &mut ConnectionL2Registrations,
) -> bool {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    // BBO-only mode rejects non-BBO subs up-front, before validation, so the
    // operator sees a single clear "denied" message in the log instead of "valid
    // subscription" then a rejection.
    if bbo_only && !matches!(&subscription, Subscription::Bbo { .. }) {
        return send_socket_message(socket, ServerResponse::Error(
            "BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed.".to_string(),
        )).await;
    }
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        return send_socket_message(socket, ServerResponse::Error(format!("Invalid subscription: {sub}"))).await;
    }

    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => match manager.subscribe(subscription.clone()) {
            Ok(inserted) => ("", inserted),
            Err(err) => {
                return send_socket_message(socket, ServerResponse::Error(format!("Rejected subscription: {err}"))).await;
            }
        },
        ClientMessage::Unsubscribe { .. } => {
            let removed = manager.unsubscribe(subscription.clone());
            // Drop the per-connection dedup/heartbeat cache entry for the just-unsubscribed
            // stream. Without this, a client that sub/unsub-cycles distinct L2 variants on
            // the same coin (or BBO across coins) leaks one entry per cycle until disconnect.
            if removed {
                match &subscription {
                    Subscription::L2Book { coin, n_sig_figs, mantissa, .. } => {
                        last_l2.remove(&l2_cache_key(coin, *n_sig_figs, *mantissa));
                        l2_registrations.unregister(&subscription);
                    }
                    Subscription::Bbo { coin } => {
                        last_bbo.remove(coin);
                    }
                    _ => {}
                }
            }
            ("un", removed)
        }
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    return send_socket_message(socket,
                        ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"))).await;
                }
            }
        } else {
            None
        };
        let subscribed = if let ClientMessage::Subscribe { subscription } = &client_message {
            Some(subscription.clone())
        } else {
            None
        };
        if !send_socket_message(socket, ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        if let Some(subscription) = &subscribed {
            l2_registrations.register(subscription);
        }
        if let Some(snapshot_msg) = snapshot_msg {
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
    bbos: &HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
) -> bool {
    let coin_key = Coin::new(coin);
    if let Some((best_bid, best_ask)) = bbos.get(&coin_key) {
        // Use the canonical wire format (Px/Sz::to_str) instead of `format!("{:?}", ...)`.
        // Debug for Px/Sz happens to produce the same output today, but going through
        // to_str matches what the L2 path already emits and skips the Formatter machinery.
        let bid = best_bid
            .as_ref()
            .map(|(px, sz, n)| crate::types::Level::new(px.to_str(), sz.to_str(), *n as usize));
        let ask = best_ask
            .as_ref()
            .map(|(px, sz, n)| crate::types::Level::new(px.to_str(), sz.to_str(), *n as usize));

        // Deduplication check
        let bid_px = bid.as_ref().map(|b| b.px().to_string()).unwrap_or_default();
        let bid_sz = bid.as_ref().map(|b| b.sz().to_string()).unwrap_or_default();
        let ask_px = ask.as_ref().map(|a| a.px().to_string()).unwrap_or_default();
        let ask_sz = ask.as_ref().map(|a| a.sz().to_string()).unwrap_or_default();
        let current = (bid_px, bid_sz, ask_px, ask_sz);

        if last_bbo.get(coin).map(|e| &e.tuple) != Some(&current) {
            BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
            BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
            let bbo = Bbo { coin: coin.to_string(), time, bid, ask };
            last_bbo.insert(
                coin.to_string(),
                BboEntry { tuple: current, last_sent: Instant::now(), payload: bbo.clone() },
            );
            return send_socket_message(socket, ServerResponse::Bbo(bbo)).await;
        }
    }
    true
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

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
    last_l2: &mut HashMap<String, L2Entry>,
    missing_is_error: bool,
) -> bool {
    match subscription {
        Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
            let snapshot = snapshot.get(&Coin::new(coin));
            if let Some(snapshot) =
                snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
            {
                let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
                let snapshot = snapshot.truncate(n_levels);
                let snapshot = snapshot.export_inner_snapshot();

                // Hash the snapshot for dedup comparison. Level derives Hash, so we
                // walk the [Vec<Level>; 2] directly - the prior `format!("{:?}", snapshot)`
                // path allocated a Debug-format string per L2 subscription per broadcast,
                // saturating glibc/jemalloc under load and dominating allocator pressure.
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                snapshot.hash(&mut hasher);
                let current_hash = hasher.finish();

                // Create unique key for this subscription (coin + params)
                let key = l2_cache_key(coin, *n_sig_figs, *mantissa);

                if last_l2.get(&key).map(|e| e.hash) != Some(current_hash) {
                    BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
                    let l2_book =
                        L2Book::from_l2_snapshot(coin.clone(), snapshot, time, *n_sig_figs, *mantissa, Some(n_levels));
                    last_l2.insert(
                        key,
                        L2Entry { hash: current_hash, last_sent: Instant::now(), payload: l2_book.clone() },
                    );
                    return send_socket_message(socket, ServerResponse::L2Book(l2_book)).await;
                }
                // else: skip, L2 unchanged
            } else {
                if missing_is_error {
                    error!("Coin {coin} not found");
                }
            }
        }
        Subscription::Bbo { coin } => {
            // Get default snapshot (no aggregation)
            let snapshot = snapshot.get(&Coin::new(coin));
            if let Some(snapshot) = snapshot.and_then(|s| s.get(&L2SnapshotParams::new(None, None))) {
                let levels = snapshot.truncate(1).export_inner_snapshot();
                let bid = levels[0].first().cloned();
                let ask = levels[1].first().cloned();

                // Only send if BBO changed (dedupe identical messages)
                let bid_px = bid.as_ref().map(|b| b.px().to_string()).unwrap_or_default();
                let bid_sz = bid.as_ref().map(|b| b.sz().to_string()).unwrap_or_default();
                let ask_px = ask.as_ref().map(|a| a.px().to_string()).unwrap_or_default();
                let ask_sz = ask.as_ref().map(|a| a.sz().to_string()).unwrap_or_default();
                let current = (bid_px, bid_sz, ask_px, ask_sz);

                if last_bbo.get(coin).map(|e| &e.tuple) != Some(&current) {
                    let bbo = Bbo { coin: coin.clone(), time, bid, ask };
                    last_bbo.insert(
                        coin.clone(),
                        BboEntry { tuple: current, last_sent: Instant::now(), payload: bbo.clone() },
                    );
                    return send_socket_message(socket, ServerResponse::Bbo(bbo)).await;
                }
                // else: skip, BBO unchanged
            }
        }
        _ => {}
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_registration_counts_distinct_levels_independently() {
        let registry = Arc::new(L2SubscriptionRegistry::default());
        let mut registrations = ConnectionL2Registrations::new(Arc::clone(&registry));
        let default_params = L2SnapshotParams::new(None, None);

        let default_levels =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: None, mantissa: None };
        let ten_levels =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: Some(10), mantissa: None };

        registrations.register(&default_levels);
        registrations.register(&ten_levels);
        registrations.unregister(&default_levels);

        assert!(registry.active_params().contains(&default_params));
        assert!(registry.active_coins().contains(&Coin::new("BTC")));

        registrations.unregister(&ten_levels);

        assert!(registry.active_params().is_empty());
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
