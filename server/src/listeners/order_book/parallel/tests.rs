use super::*;

fn stream_test_dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("orderbook-server-{name}-{}", std::process::id()));
    drop(std::fs::remove_dir_all(&path));
    std::fs::create_dir_all(&path).expect("test stream directory should exist");
    path
}

fn append_to_file(path: &PathBuf, contents: &str) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).expect("test stream file should open");
    std::io::Write::write_all(&mut file, contents.as_bytes()).expect("test stream file should append");
}

fn features(value: &str) -> FeatureSet {
    value.parse().expect("valid features")
}

fn diff_at(height: u64) -> FileEvent {
    FileEvent::OrderDiff(format!(r#"{{"block_number":{height}}}"#))
}

fn status_at(height: u64) -> FileEvent {
    FileEvent::OrderStatus(format!(r#"{{"block_number":{height}}}"#))
}

#[tokio::test]
async fn merge_waits_for_a_readers_next_batch_without_losing_heads_on_cancellation() {
    let (senders, mut rx) = file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses], 8);
    senders[0].try_send(diff_at(20)).unwrap();
    let mut ready = Vec::new();
    assert!(tokio::time::timeout(Duration::from_millis(10), rx.recv_many(&mut ready, 8)).await.is_err());
    assert!(ready.is_empty());

    senders[1].try_send(status_at(10)).unwrap();
    assert!(senders[1].send(SourceMessage::CaughtUp));
    assert!(senders[0].send(SourceMessage::CaughtUp));
    assert_eq!(rx.recv_many(&mut ready, 8).await, 2);
    assert!(matches!(&ready[0], FileEvent::OrderStatus(_)));
    assert!(matches!(&ready[1], FileEvent::OrderDiff(_)));

    ready.clear();
    senders[0].try_send(diff_at(21)).unwrap();
    assert!(senders[0].send(SourceMessage::CaughtUp));
    assert_eq!(rx.recv_many(&mut ready, 8).await, 1, "a quiet status stream must not stall diffs");
    drop(senders);
    assert_eq!(rx.recv_many(&mut ready, 8).await, 0);
}

#[tokio::test]
async fn merge_preserves_source_order_and_alternates_equal_height_lines() {
    let (senders, mut rx) = file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses], 16);
    for event in [diff_at(10), diff_at(10), diff_at(12)] {
        senders[0].try_send(event).unwrap();
    }
    for event in [status_at(10), status_at(10), status_at(11)] {
        senders[1].try_send(event).unwrap();
    }
    drop(senders);
    let mut ready = Vec::new();
    assert_eq!(rx.recv_many(&mut ready, 16).await, 6);
    assert!(matches!(&ready[0], FileEvent::OrderDiff(_)));
    assert!(matches!(&ready[1], FileEvent::OrderStatus(_)));
    assert!(matches!(&ready[2], FileEvent::OrderDiff(_)));
    assert!(matches!(&ready[3], FileEvent::OrderStatus(_)));
    assert!(matches!(&ready[4], FileEvent::OrderStatus(line) if line.contains("11")));
    assert!(matches!(&ready[5], FileEvent::OrderDiff(line) if line.contains("12")));
}

#[tokio::test]
async fn merge_checks_for_new_backlog_after_an_eof_marker() {
    let (senders, mut rx) = file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses], 8);
    assert!(senders[0].send(SourceMessage::CaughtUp));
    senders[0].try_send(diff_at(10)).unwrap();
    senders[1].try_send(status_at(20)).unwrap();
    drop(senders);
    let mut ready = Vec::new();
    assert_eq!(rx.recv_many(&mut ready, 8).await, 2);
    assert!(matches!(&ready[0], FileEvent::OrderDiff(_)));
    assert!(matches!(&ready[1], FileEvent::OrderStatus(_)));
}

#[tokio::test]
async fn merge_keeps_gaps_and_malformed_lines_for_the_repair_path() {
    let (senders, mut rx) = file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses], 8);
    senders[0].try_send(FileEvent::ContinuityLost(EventSource::OrderDiffs)).unwrap();
    senders[0].try_send(FileEvent::OrderDiff("malformed".to_string())).unwrap();
    senders[0].try_send(diff_at(10)).unwrap();
    drop(senders);
    let mut ready = Vec::new();
    assert_eq!(rx.recv_many(&mut ready, 8).await, 3);
    assert!(matches!(&ready[0], FileEvent::ContinuityLost(EventSource::OrderDiffs)));
    assert!(matches!(&ready[1], FileEvent::OrderDiff(line) if line == "malformed"));
    assert!(matches!(&ready[2], FileEvent::OrderDiff(line) if line.contains("10")));
}

#[tokio::test]
async fn fills_do_not_wait_for_book_readers_and_do_not_hold_up_book_events() {
    let (senders, mut rx) =
        file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses, EventSource::Fills], 12);
    senders[2].try_send(FileEvent::Fill(r#"{"block_number":10}"#.to_string())).unwrap();
    let mut ready = Vec::new();
    assert_eq!(rx.recv_many(&mut ready, 8).await, 1);
    assert!(matches!(&ready[0], FileEvent::Fill(_)));

    ready.clear();
    senders[0].try_send(diff_at(11)).unwrap();
    assert!(senders[0].send(SourceMessage::CaughtUp));
    assert!(senders[1].send(SourceMessage::CaughtUp));
    assert_eq!(rx.recv_many(&mut ready, 8).await, 1);
    assert!(matches!(&ready[0], FileEvent::OrderDiff(_)));
}

#[test]
fn readers_report_eof_after_submitting_available_lines() {
    let base_dir = stream_test_dir("reader-eof");
    let day_dir = base_dir.join("hourly/20260916");
    std::fs::create_dir_all(&day_dir).unwrap();
    let path = day_dir.join("19");
    std::fs::write(&path, "").unwrap();
    let mut reader = FileReader::new(base_dir.clone());
    reader.start_tracking(&path);
    append_to_file(&path, "{\"block_number\":10}\n");
    let (mut senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs], 2);
    let tx = senders.remove(0);
    let rx = &mut receiver.sources[0].rx;
    let sink = FileLineSink::Events { source: EventSource::OrderDiffs, tx };
    assert!(submit_available(&sink, reader.read_tracked(), &mut false));
    assert!(matches!(rx.try_recv(), Ok(SourceMessage::Lines(_))));
    assert!(rx.try_recv().is_err(), "the node may have appended while the previous read was sent");
    assert!(submit_available(&sink, reader.read_tracked(), &mut false));
    assert!(matches!(rx.try_recv(), Ok(SourceMessage::CaughtUp)));
    std::fs::remove_dir_all(base_dir).unwrap();
}

#[test]
fn enabled_sources_for_bbo_need_book_state_inputs() {
    assert_eq!(enabled_event_sources(features("bbo")), vec![EventSource::OrderStatuses, EventSource::OrderDiffs]);
    assert_eq!(enabled_event_sources(features("allbbo")), vec![EventSource::OrderStatuses, EventSource::OrderDiffs]);
}

#[test]
fn enabled_sources_for_trades_only_watch_fills() {
    assert_eq!(enabled_event_sources(features("trades")), vec![EventSource::Fills]);
}

#[test]
fn enabled_sources_for_raw_order_streams_are_granular() {
    assert_eq!(enabled_event_sources(features("bookdiffs")), vec![EventSource::OrderDiffs]);
    assert_eq!(enabled_event_sources(features("orderupdates")), vec![EventSource::OrderStatuses]);
}

#[test]
fn enabled_sources_for_stats_watch_fills_and_order_diffs_without_order_statuses() {
    assert_eq!(enabled_event_sources(features("stats")), vec![EventSource::Fills, EventSource::OrderDiffs]);
    assert!(!features("stats").requires_book_state());
    assert!(!features("stats").watch_order_statuses());
}

#[test]
fn ordersync_only_watches_fills_without_full_fill_batches() {
    let order_sync = features("ordersync");
    assert_eq!(enabled_event_sources(order_sync), vec![EventSource::Fills]);
    assert!(order_sync.watch_fills());
    assert!(!order_sync.needs_fill_batches());
    assert!(!order_sync.requires_book_state());
}

#[test]
fn bbo_and_ordersync_fill_lines_bypass_the_shared_event_queue() {
    let (mut senders, mut receiver) = file_event_channels(&[EventSource::Fills], 1);
    let tx = senders.remove(0);
    let rx = &mut receiver.sources[0].rx;
    let recorder = OrderSyncRecorder::default();
    let sink = file_line_sink(EventSource::Fills, features("bbo,ordersync"), tx, Some(recorder.clone()))
        .expect("ordersync recorder creates a fill sink");

    assert!(sink.submit(r#"{"events":[[null,{"time":300000}]]}"#.to_string()));
    assert_eq!(recorder.status_at(600_000).last_order_at, Some(300));
    assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
}

#[test]
fn trades_and_ordersync_keep_the_existing_full_fill_path() {
    let (mut senders, mut receiver) = file_event_channels(&[EventSource::Fills], 1);
    let tx = senders.remove(0);
    let rx = &mut receiver.sources[0].rx;
    let sink = file_line_sink(EventSource::Fills, features("trades,ordersync"), tx, Some(OrderSyncRecorder::default()))
        .expect("fill sink exists");

    assert!(sink.submit("fill line".to_string()));
    assert!(matches!(rx.try_recv(), Ok(SourceMessage::Lines(batch)) if batch.lines.as_slice() == ["fill line"]));
}

#[test]
fn continuity_loss_precedes_recovered_lines_on_the_shared_channel() {
    let (mut senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs], 2);
    let tx = senders.remove(0);
    let rx = &mut receiver.sources[0].rx;
    let sink = file_line_sink(EventSource::OrderDiffs, features("bbo"), tx, None).expect("diff sink exists");

    assert!(sink.submit_continuity_loss());
    assert!(sink.submit("recovered line".to_string()));

    assert!(matches!(rx.try_recv(), Ok(SourceMessage::ContinuityLost)));
    assert!(matches!(rx.try_recv(), Ok(SourceMessage::Lines(batch)) if batch.lines.as_slice() == ["recovered line"]));
}
#[test]
fn notifications_coalesce_without_losing_rescan_or_error_flags() {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let signal = WatcherSignal { flags: AtomicU8::new(0), tx };
    for _ in 0..100_000 {
        signal.notify(NOTIFY_DATA);
    }
    signal.notify(NOTIFY_RESCAN | NOTIFY_ERROR);
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_err());
    assert_eq!(signal.take(), NOTIFY_DATA | NOTIFY_RESCAN | NOTIFY_ERROR);
    assert_eq!(signal.take(), 0);
    signal.notify(NOTIFY_DATA);
    assert!(rx.try_recv().is_ok());
}

#[test]
fn partial_data_does_not_declare_eof_and_repeated_idle_is_coalesced() {
    let base_dir = stream_test_dir("partial-eof");
    let day_dir = base_dir.join("hourly/20260916");
    std::fs::create_dir_all(&day_dir).unwrap();
    let path = day_dir.join("19");
    std::fs::write(&path, "").unwrap();
    let mut reader = FileReader::new(base_dir.clone());
    reader.start_tracking(&path);
    append_to_file(&path, "{\"block_number\":10}");
    let (mut senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs], 4);
    let sink = FileLineSink::Events { source: EventSource::OrderDiffs, tx: senders.remove(0) };
    let rx = &mut receiver.sources[0].rx;
    let mut caught_up = false;
    assert!(submit_available(&sink, reader.read_tracked(), &mut caught_up));
    assert!(rx.try_recv().is_err());
    assert!(submit_available(&sink, reader.read_tracked(), &mut caught_up));
    assert!(matches!(rx.try_recv(), Ok(SourceMessage::CaughtUp)));
    assert!(submit_available(&sink, reader.read_tracked(), &mut caught_up));
    assert!(rx.try_recv().is_err());
    append_to_file(&path, "\n");
    assert!(submit_available(&sink, reader.read_tracked(), &mut caught_up));
    assert!(matches!(rx.try_recv(), Ok(SourceMessage::Lines(_))));
    assert!(!caught_up);
    std::fs::remove_dir_all(base_dir).unwrap();
}

#[tokio::test]
async fn byte_budget_stalls_reader_while_appends_continue_and_releases_after_drain() {
    let base_dir = stream_test_dir("byte-budget");
    let day_dir = base_dir.join("hourly/20260916");
    std::fs::create_dir_all(&day_dir).unwrap();
    let path = day_dir.join("19");
    std::fs::write(&path, "").unwrap();
    let mut reader = FileReader::new(base_dir.clone());
    reader.start_tracking(&path);
    append_to_file(&path, "{\"block_number\":1}\n");

    let budget = Arc::new(QueueBudget {
        state: Mutex::new(BudgetState::default()),
        space: Condvar::new(),
        limit: 200,
        lines: AtomicUsize::new(0),
        source: "diffs",
    });
    let (tx, rx) = channel(8);
    let sender = SourceSender { tx, budget: Arc::clone(&budget) };
    let mut receiver = FileEventReceiver {
        sources: vec![SourceQueue {
            source: EventSource::OrderDiffs,
            rx,
            budget: Arc::clone(&budget),
            batch: None,
            head: None,
            caught_up: false,
            closed: false,
        }],
        next_source: 0,
    };
    let sink = FileLineSink::Events { source: EventSource::OrderDiffs, tx: sender };
    assert!(submit_file_read(&sink, reader.read_tracked()));
    append_to_file(&path, "{\"block_number\":2}\n");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let producer = thread::spawn(move || {
        let read = reader.read_tracked();
        started_tx.send(()).unwrap();
        let result = submit_file_read(&sink, read);
        done_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
    append_to_file(&path, "{\"block_number\":3}\n");
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 3);
    assert!(budget.state.lock().unwrap().used <= 200);
    let mut ready = Vec::new();
    assert_eq!(receiver.recv_many(&mut ready, 1).await, 1);
    assert!(done_rx.try_recv().is_err(), "the receiver still owns the batch lease");
    assert_eq!(tokio::time::timeout(Duration::from_secs(1), receiver.recv_many(&mut ready, 8)).await.unwrap(), 1);
    assert!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    producer.join().unwrap();
    assert_eq!(ready.len(), 2);
    assert_eq!(budget.state.lock().unwrap().used, 0);
    std::fs::remove_dir_all(base_dir).unwrap();
}

#[test]
fn receiver_drop_wakes_a_producer_waiting_for_byte_budget() {
    let (mut senders, receiver) = file_event_channels(&[EventSource::OrderDiffs], 8);
    let sender = senders.remove(0);
    let budget = Arc::clone(&sender.budget);
    let lease = budget.acquire(QUEUE_BYTES_PER_SOURCE, false).unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let producer = thread::spawn(move || done_tx.send(sender.send_lines(vec!["line".to_owned()])).unwrap());
    assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(receiver);
    assert!(!done_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    producer.join().unwrap();
    drop(lease);
    assert_eq!(budget.state.lock().unwrap().used, 0);
}

#[tokio::test]
async fn batches_preserve_height_order_and_receive_turns_bound_bytes() {
    let (senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs, EventSource::OrderStatuses], 8);
    let line =
        |height| format!("{{\"block_number\":{height},\"payload\":\"{}\"}}", "x".repeat(RECEIVE_BYTES_PER_TURN / 2));
    assert!(senders[0].send_lines(vec![line(1), line(3)]));
    assert!(senders[1].send_lines(vec![line(2), line(4)]));
    drop(senders);
    let mut ready = Vec::new();
    assert_eq!(receiver.recv_many(&mut ready, 100).await, 2);
    assert!(matches!(&ready[0], FileEvent::OrderDiff(line) if line.starts_with("{\"block_number\":1,")));
    assert!(matches!(&ready[1], FileEvent::OrderStatus(line) if line.starts_with("{\"block_number\":2,")));
    assert_eq!(receiver.recv_many(&mut ready, 100).await, 2);
    assert!(matches!(&ready[2], FileEvent::OrderDiff(line) if line.starts_with("{\"block_number\":3,")));
    assert!(matches!(&ready[3], FileEvent::OrderStatus(line) if line.starts_with("{\"block_number\":4,")));
    assert_eq!(receiver.recv_many(&mut ready, 100).await, 0);
}
#[tokio::test]
async fn real_watcher_drains_bursts_and_rotation_after_consumer_stalls() {
    let base_dir = stream_test_dir("watcher-burst");
    let day_dir = base_dir.join("hourly/20260916");
    std::fs::create_dir_all(&day_dir).unwrap();
    let path = day_dir.join("19");
    std::fs::write(&path, "").unwrap();
    let (mut senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs], 1);
    let sink = FileLineSink::Events { source: EventSource::OrderDiffs, tx: senders.remove(0) };
    let (watcher, ready) = spawn_file_watcher(base_dir.clone(), sink);
    ready.await.unwrap();
    let initial = tokio::time::timeout(Duration::from_secs(5), receiver.sources[0].rx.recv()).await.unwrap();
    assert!(matches!(initial, Some(SourceMessage::CaughtUp)));
    let line = |index| format!("{{\"block_number\":{index},\"payload\":\"{}\"}}", "x".repeat(1024));
    let mut first = String::new();
    for index in 0..2000 {
        first.push_str(&line(index));
        first.push('\n');
    }
    append_to_file(&path, &first);
    std::fs::write(day_dir.join("20"), format!("{}\n", line(2000))).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut expected = 0;
        let mut ready = Vec::new();
        while expected <= 2000 {
            ready.clear();
            assert!(receiver.recv_many(&mut ready, 128).await > 0);
            for event in &ready {
                assert!(
                    matches!(event, FileEvent::OrderDiff(actual) if actual == &line(expected)),
                    "unexpected event: {event:?}"
                );
                expected += 1;
            }
        }
        assert_eq!(expected, 2001);
    })
    .await;
    drop(receiver);
    watcher.join().unwrap();
    result.unwrap();
    std::fs::remove_dir_all(base_dir).unwrap();
}

#[test]
fn first_file_after_watcher_start_is_read_from_the_beginning() {
    let base_dir = stream_test_dir("late-first-file");
    let day_dir = base_dir.join("hourly/20260916");
    std::fs::create_dir_all(&day_dir).unwrap();
    let mut reader = FileReader::new(base_dir.clone());
    assert_eq!(read_available(&mut reader, true).progress, ReadProgress::Idle);
    std::fs::write(day_dir.join("19"), "{\"block_number\":1}\n").unwrap();
    let mut lines = Vec::new();
    for _ in 0..3 {
        lines.extend(read_available(&mut reader, true).lines);
    }
    assert_eq!(lines, ["{\"block_number\":1}"]);
    std::fs::remove_dir_all(base_dir).unwrap();
}

#[tokio::test]
async fn startup_failure_is_returned_instead_of_serving_an_incomplete_book() {
    let base = stream_test_dir("startup-failure");
    let result = start_parallel_file_watchers(base.clone(), features("bbo"), None).await;
    assert!(result.is_err());
    std::fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn startup_attaches_all_sources_before_returning_to_the_snapshot_scheduler() {
    let base = stream_test_dir("startup-barrier");
    let sources = [EventSource::OrderStatuses, EventSource::OrderDiffs];
    let paths: Vec<_> = sources
        .iter()
        .map(|source| {
            let day = source.event_source_dir_streaming(&base).join("hourly/20260916");
            std::fs::create_dir_all(&day).unwrap();
            let path = day.join("19");
            std::fs::write(&path, "history\n").unwrap();
            path
        })
        .collect();
    let (mut receiver, handles) = start_parallel_file_watchers(base.clone(), features("bbo"), None).await.unwrap();
    for path in &paths {
        append_to_file(path, "{\"block_number\":42}\n");
    }
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut ready = Vec::new();
        while ready.len() < 2 {
            assert!(receiver.recv_many(&mut ready, 2).await > 0);
        }
        for event in ready {
            match event {
                FileEvent::OrderStatus(line) | FileEvent::OrderDiff(line) => assert_eq!(line, "{\"block_number\":42}"),
                other => panic!("unexpected event: {other:?}"),
            }
        }
    })
    .await;
    drop(receiver);
    for handle in handles {
        handle.join().unwrap();
    }
    result.unwrap();
    std::fs::remove_dir_all(base).unwrap();
}
