use super::*;

const BLOCK_10_TIME_MS: u64 = 1_705_314_600_010;
const BLOCK_11_TIME_MS: u64 = 1_705_314_600_011;

fn btc_bid_line_in_block(oid: u64, sz: &str, block_number: u64) -> String {
    serde_json::json!({
        "local_time": "2024-01-15T10:30:00.000000000",
        "block_time": format!("2024-01-15T10:30:00.{block_number:03}000000"),
        "block_number": block_number,
        "events": [{
            "user": "0x0000000000000000000000000000000000000000",
            "oid": oid,
            "side": "B",
            "px": "100",
            "coin": "BTC",
            "raw_book_diff": { "new": { "sz": sz } }
        }]
    })
    .to_string()
}

fn listener_at_block_10(tx: Sender<Arc<InternalMessage>>, features: FeatureSet) -> OrderBookListener {
    let mut listener = OrderBookListener::new(Some(tx), false, features);
    let state =
        order_book_state_with_bids_and_time(&[("BTC", &[(1, "100", "1")])], 10, BLOCK_10_TIME_MS, BookMode::DiffsOnly);
    listener.replace_order_book_state(state, 10, 10, true);
    listener.l2_subscription_registry.register_l2(default_l2_key("BTC"));
    listener.allbbo_subscription_registry.register();
    listener
}

fn apply_btc_bid(listener: &mut OrderBookListener, oid: u64, block_number: u64) {
    listener.process_data_hft(btc_bid_line_in_block(oid, "1", block_number), EventSource::OrderDiffs).unwrap();
}

fn flush_l2_on_timer(listener: &mut OrderBookListener) {
    listener.last_l2_broadcast = Some(Instant::now() - Duration::from_millis(L2_BROADCAST_THROTTLE_MS + 1));
    listener.flush_due_publications();
}

fn flush_allbbo_on_timer(listener: &mut OrderBookListener) {
    listener.last_allbbo_broadcast = Some(Instant::now() - Duration::from_millis(ALLBBO_BROADCAST_THROTTLE_MS + 1));
    listener.flush_due_publications();
}

fn bid_summary(bid: Option<&(Px, Sz, u32)>) -> (String, String, u32) {
    let (px, sz, n) = bid.expect("BTC bid");
    (px.to_str(), sz.to_str(), *n)
}

/// The BTC best bid of every book-derived message, as (feed, time, px, sz, n).
fn published_btc_bids(rx: &mut Receiver<Arc<InternalMessage>>) -> Vec<(&'static str, u64, String, String, u32)> {
    drain_all(rx)
        .iter()
        .filter_map(|msg| match msg.as_ref() {
            InternalMessage::L2Update { l2_books } => {
                let payload = serde_json::to_value(l2_books.get(&default_l2_key("BTC"))?.payload()).unwrap();
                let level = &payload["levels"][0][0];
                Some((
                    "l2",
                    payload["time"].as_u64().unwrap(),
                    level["px"].as_str().unwrap().to_string(),
                    level["sz"].as_str().unwrap().to_string(),
                    u32::try_from(level["n"].as_u64().unwrap()).unwrap(),
                ))
            }
            InternalMessage::BboUpdate { bbos, time } => {
                let (px, sz, n) = bid_summary(bbos.get(&Coin::new("BTC"))?.0.as_ref());
                Some(("bbo", *time, px, sz, n))
            }
            InternalMessage::AllBboUpdate { bbos, time } => {
                let (_, (bid, _)) = bbos.iter().find(|(coin, _)| *coin == Coin::new("BTC"))?;
                let (px, sz, n) = bid_summary(bid.as_ref());
                Some(("allbbo", *time, px, sz, n))
            }
            _ => None,
        })
        .collect()
}

fn block_11_messages_are_complete(published: &[(&'static str, u64, String, String, u32)]) -> bool {
    published
        .iter()
        .filter(|(_, time, ..)| *time == BLOCK_11_TIME_MS)
        .all(|(_, _, px, sz, n)| (px.as_str(), sz.as_str(), *n) == ("100", "3", 3))
}

#[test]
fn l2_does_not_publish_a_block_before_its_last_line() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("l2book"));

    apply_btc_bid(&mut listener, 2, 11);
    flush_l2_on_timer(&mut listener);
    apply_btc_bid(&mut listener, 3, 11);
    flush_l2_on_timer(&mut listener);

    let published = published_btc_bids(&mut rx);
    assert!(block_11_messages_are_complete(&published), "published {published:?}");
}

#[test]
fn l2_publishes_the_book_after_a_whole_block() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("l2book"));

    apply_btc_bid(&mut listener, 2, 11);
    flush_l2_on_timer(&mut listener);
    apply_btc_bid(&mut listener, 3, 11);
    apply_btc_bid(&mut listener, 4, 12);
    flush_l2_on_timer(&mut listener);

    assert_eq!(published_btc_bids(&mut rx), vec![("l2", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3)]);
}

#[test]
fn bbo_does_not_publish_a_block_before_its_last_line() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo"));

    apply_btc_bid(&mut listener, 2, 11);
    apply_btc_bid(&mut listener, 3, 11);

    let published = published_btc_bids(&mut rx);
    assert!(block_11_messages_are_complete(&published), "published {published:?}");
}

#[test]
fn bbo_publishes_the_book_after_a_whole_block() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo"));

    apply_btc_bid(&mut listener, 2, 11);
    apply_btc_bid(&mut listener, 3, 11);
    apply_btc_bid(&mut listener, 4, 12);

    assert_eq!(published_btc_bids(&mut rx), vec![("bbo", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3)]);
}

#[test]
fn allbbo_does_not_publish_a_block_before_its_last_line() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("allbbo"));

    apply_btc_bid(&mut listener, 2, 11);
    flush_allbbo_on_timer(&mut listener);
    apply_btc_bid(&mut listener, 3, 11);
    flush_allbbo_on_timer(&mut listener);

    let published = published_btc_bids(&mut rx);
    assert!(block_11_messages_are_complete(&published), "published {published:?}");
}

#[test]
fn allbbo_publishes_the_book_after_a_whole_block() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("allbbo"));

    apply_btc_bid(&mut listener, 2, 11);
    flush_allbbo_on_timer(&mut listener);
    apply_btc_bid(&mut listener, 3, 11);
    apply_btc_bid(&mut listener, 4, 12);
    flush_allbbo_on_timer(&mut listener);

    assert_eq!(published_btc_bids(&mut rx), vec![("allbbo", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3)]);
}

#[test]
fn a_block_publishes_once_book_streams_stay_idle_for_the_quiet_window() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo,l2book"));
    apply_btc_bid(&mut listener, 2, 11);
    apply_btc_bid(&mut listener, 3, 11);

    let idle_at = Instant::now();
    listener.block_gate.on_books_idle(idle_at);
    listener.close_quiet_block(idle_at + Duration::from_millis(1), &drained_file_events());
    flush_l2_on_timer(&mut listener);
    assert_eq!(published_btc_bids(&mut rx), vec![]);

    listener.close_quiet_block(idle_at + Duration::from_millis(2), &drained_file_events());
    flush_l2_on_timer(&mut listener);
    assert_eq!(
        published_btc_bids(&mut rx),
        vec![
            ("bbo", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3),
            ("l2", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3),
        ]
    );
}

#[tokio::test]
async fn the_quiet_timer_leaves_a_block_open_while_book_lines_are_queued() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo"));
    apply_btc_bid(&mut listener, 2, 11);
    let (senders, mut file_events) =
        parallel::file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses], 4);
    senders[0].try_send(parallel::FileEvent::OrderDiff(btc_bid_line_in_block(3, "1", 11))).unwrap();

    let idle_at = Instant::now();
    listener.block_gate.on_books_idle(idle_at);
    listener.close_quiet_block(idle_at + block_gate::BLOCK_QUIET, &file_events);
    assert_eq!(published_btc_bids(&mut rx), vec![], "the queued block 11 line has not applied");

    drop(senders);
    let mut ready = Vec::new();
    assert_eq!(file_events.recv_many(&mut ready, 8).await.events, 1);
    let Some(parallel::FileEvent::OrderDiff(line)) = ready.pop() else { panic!("expected the queued diff") };
    listener.apply_parsed_hft(parse_hft_event(line, EventSource::OrderDiffs).unwrap().unwrap());
    close_block_after_quiet(&mut listener);
    assert_eq!(published_btc_bids(&mut rx), vec![("bbo", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3)]);
}

#[test]
fn a_snapshot_installed_while_book_streams_are_idle_publishes_after_the_quiet_window() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo"));
    listener.begin_snapshot_replay();
    let state =
        order_book_state_with_bids_and_time(&[("BTC", &[(1, "100", "2")])], 10, BLOCK_10_TIME_MS, BookMode::DiffsOnly);
    listener.install_snapshot_state(state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");
    assert_eq!(published_btc_bids(&mut rx), vec![]);

    listener.close_quiet_block(Instant::now() + block_gate::BLOCK_QUIET, &drained_file_events());
    assert_eq!(published_btc_bids(&mut rx), vec![("bbo", BLOCK_10_TIME_MS, "100".to_string(), "2".to_string(), 1)]);
}

#[test]
fn a_snapshot_install_keeps_open_a_rewritten_block_its_journal_ended_in() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo"));
    listener.begin_snapshot_replay();
    apply_btc_bid(&mut listener, 2, 14);
    apply_btc_bid(&mut listener, 3, 13);
    let state =
        order_book_state_with_bids_and_time(&[("BTC", &[(1, "100", "1")])], 10, BLOCK_10_TIME_MS, BookMode::DiffsOnly);
    drain_all(&mut rx);
    listener.install_snapshot_state(state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");

    apply_btc_bid(&mut listener, 4, 13);
    assert_eq!(published_btc_bids(&mut rx), vec![], "block 13 is still being rewritten");

    close_block_after_quiet(&mut listener);
    assert_eq!(published_btc_bids(&mut rx), vec![("bbo", 1_705_314_600_014, "100".to_string(), "4".to_string(), 4)]);
}

#[test]
fn a_book_line_after_a_snapshot_install_keeps_its_block_open() {
    let (tx, mut rx) = channel(64);
    let mut listener = listener_at_block_10(tx, features("bbo"));
    listener.begin_snapshot_replay();
    apply_btc_bid(&mut listener, 2, 11);
    let state =
        order_book_state_with_bids_and_time(&[("BTC", &[(1, "100", "1")])], 10, BLOCK_10_TIME_MS, BookMode::DiffsOnly);
    let outcome = listener.install_snapshot_state(state, 10, SnapshotTaskKind::Refresh).expect("refresh installs");
    assert_eq!(outcome, SnapshotInstallOutcome::Installed { replayed_lines: 1 });

    apply_btc_bid(&mut listener, 3, 11);
    listener.close_quiet_block(Instant::now() + block_gate::BLOCK_QUIET, &drained_file_events());
    assert_eq!(published_btc_bids(&mut rx), vec![], "block 11 got a line after the install");

    close_block_after_quiet(&mut listener);
    assert_eq!(published_btc_bids(&mut rx), vec![("bbo", BLOCK_11_TIME_MS, "100".to_string(), "3".to_string(), 3)]);
}
