#[cfg(test)]
mod benchmarks;
mod reader;

use reader::{FileContinuity, FileRead, FileReader, ReadProgress};

use std::{
    future::poll_fn,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use sonic_rs::JsonValueTrait;
use tokio::sync::mpsc::{Receiver, Sender, channel, error::TrySendError};

use crate::{
    FeatureSet,
    metrics::{
        EVENTS_PROCESSED_TOTAL, FILE_BACKPRESSURE_SECONDS_TOTAL, FILE_EVENTS_TOTAL, FILE_LINES_PARSED_TOTAL,
        FILE_QUEUE_BYTES, FILE_READ_BYTES_TOTAL, FILE_READ_CALLS_TOTAL, FILE_READ_DURATION, FILE_UNREAD_BYTES,
        FILE_WATCHER_WAKEUPS_TOTAL, PARSE_ERRORS_TOTAL,
    },
    order_sync::OrderSyncRecorder,
    types::node_data::EventSource,
};

static ORDER_SYNC_PARSE_ERR_COUNT: AtomicU64 = AtomicU64::new(0);
const QUEUE_BYTES_PER_SOURCE: usize = 32 * 1024 * 1024;
const RECEIVE_BYTES_PER_TURN: usize = 256 * 1024;

#[derive(Debug)]
pub(crate) enum FileEvent {
    OrderStatus(String),
    OrderDiff(String),
    Fill(String),
    ContinuityLost(EventSource),
}

#[derive(Default)]
struct BudgetState {
    used: usize,
    closed: bool,
}

struct QueueBudget {
    state: Mutex<BudgetState>,
    space: Condvar,
    limit: usize,
    lines: AtomicUsize,
    source: &'static str,
}

impl QueueBudget {
    fn acquire(self: &Arc<Self>, bytes: usize, wait: bool) -> Option<ByteLease> {
        if bytes > self.limit {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut started = None;
        while !state.closed && state.used + bytes > self.limit {
            if !wait {
                return None;
            }
            started.get_or_insert_with(Instant::now);
            state = self.space.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if let Some(started) = started {
            FILE_BACKPRESSURE_SECONDS_TOTAL.with_label_values(&[self.source]).inc_by(started.elapsed().as_secs_f64());
        }
        if state.closed {
            return None;
        }
        state.used += bytes;
        FILE_QUEUE_BYTES.with_label_values(&[self.source]).set(i64::try_from(state.used).unwrap_or(i64::MAX));
        drop(state);
        Some(ByteLease { budget: Arc::clone(self), bytes })
    }
}

struct ByteLease {
    budget: Arc<QueueBudget>,
    bytes: usize,
}

impl Drop for ByteLease {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.used -= self.bytes;
        FILE_QUEUE_BYTES.with_label_values(&[self.budget.source]).set(i64::try_from(state.used).unwrap_or(i64::MAX));
        drop(state);
        self.budget.space.notify_one();
    }
}

struct QueuedLines {
    lines: std::vec::IntoIter<String>,
    lease: ByteLease,
}

impl QueuedLines {
    fn next(&mut self) -> Option<String> {
        let line = self.lines.next()?;
        self.lease.budget.lines.fetch_sub(1, AtomicOrdering::Relaxed);
        Some(line)
    }
}

impl Drop for QueuedLines {
    fn drop(&mut self) {
        self.lease.budget.lines.fetch_sub(self.lines.len(), AtomicOrdering::Relaxed);
    }
}

enum SourceMessage {
    Lines(QueuedLines),
    ContinuityLost,
    CaughtUp,
}

#[derive(Clone)]
pub(super) struct SourceSender {
    tx: Sender<SourceMessage>,
    budget: Arc<QueueBudget>,
}

impl SourceSender {
    fn batch(&self, lines: Vec<String>, wait: bool) -> Option<SourceMessage> {
        let bytes = lines.capacity() * size_of::<String>() + lines.iter().map(String::capacity).sum::<usize>();
        let lease = self.budget.acquire(bytes, wait)?;
        self.budget.lines.fetch_add(lines.len(), AtomicOrdering::Relaxed);
        Some(SourceMessage::Lines(QueuedLines { lines: lines.into_iter(), lease }))
    }

    fn send(&self, message: SourceMessage) -> bool {
        match self.tx.try_send(message) {
            Ok(()) => true,
            Err(TrySendError::Closed(_)) => false,
            Err(TrySendError::Full(message)) => {
                let started = Instant::now();
                let sent = self.tx.blocking_send(message).is_ok();
                FILE_BACKPRESSURE_SECONDS_TOTAL
                    .with_label_values(&[self.budget.source])
                    .inc_by(started.elapsed().as_secs_f64());
                sent
            }
        }
    }

    pub(super) fn send_lines(&self, lines: Vec<String>) -> bool {
        lines.is_empty() || self.batch(lines, true).is_some_and(|batch| self.send(batch))
    }

    #[cfg(test)]
    pub(super) fn try_send(&self, event: FileEvent) -> Result<(), &'static str> {
        let message = match event {
            FileEvent::OrderStatus(line) | FileEvent::OrderDiff(line) | FileEvent::Fill(line) => {
                self.batch(vec![line], false).ok_or("byte budget exhausted or closed")?
            }
            FileEvent::ContinuityLost(_) => SourceMessage::ContinuityLost,
        };
        self.tx.try_send(message).map_err(|_| "batch queue full or closed")
    }
}

struct SourceQueue {
    source: EventSource,
    rx: Receiver<SourceMessage>,
    budget: Arc<QueueBudget>,
    batch: Option<QueuedLines>,
    head: Option<(u64, FileEvent)>,
    caught_up: bool,
    closed: bool,
}

impl SourceQueue {
    fn poll_head(&mut self, cx: &mut Context<'_>) {
        while self.head.is_none() && !self.closed {
            if let Some(line) = self.batch.as_mut().and_then(QueuedLines::next) {
                let height = sonic_rs::get_from_str(&line, ["block_number"]).ok().and_then(|v| v.as_u64()).unwrap_or(0);
                let event = match self.source {
                    EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                    EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                    EventSource::Fills => FileEvent::Fill(line),
                };
                self.head = Some((height, event));
                self.caught_up = false;
                break;
            }
            self.batch = None;
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(SourceMessage::Lines(batch))) => self.batch = Some(batch),
                Poll::Ready(Some(SourceMessage::CaughtUp)) => self.caught_up = true,
                Poll::Ready(Some(SourceMessage::ContinuityLost)) => {
                    self.head = Some((0, FileEvent::ContinuityLost(self.source)));
                    self.caught_up = false;
                }
                Poll::Ready(None) => self.closed = true,
                Poll::Pending => break,
            }
        }
    }
}

impl Drop for SourceQueue {
    fn drop(&mut self) {
        self.budget.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).closed = true;
        self.budget.space.notify_all();
    }
}

/// Merge book streams by height while they have unread data. An EOF marker lets
/// the other streams keep moving when a source has no more events available.
pub(crate) struct FileEventReceiver {
    sources: Vec<SourceQueue>,
    next_source: usize,
}

impl FileEventReceiver {
    pub(super) fn len(&self) -> usize {
        self.sources
            .iter()
            .map(|source| source.budget.lines.load(AtomicOrdering::Relaxed) + usize::from(source.head.is_some()))
            .sum()
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<FileEvent>> {
        for source in &mut self.sources {
            source.poll_head(cx);
        }
        let waiting_for_book = self.sources.iter().any(|source| {
            source.source != EventSource::Fills && source.head.is_none() && !source.caught_up && !source.closed
        });
        let selected = (0..self.sources.len())
            .map(|offset| (self.next_source + offset) % self.sources.len())
            .filter(|&index| !waiting_for_book || self.sources[index].source == EventSource::Fills)
            .filter_map(|index| self.sources[index].head.as_ref().map(|(height, _)| (index, *height)))
            .min_by_key(|(_, height)| *height);
        if let Some((index, _)) = selected {
            self.next_source = (index + 1) % self.sources.len();
            return Poll::Ready(self.sources[index].head.take().map(|(_, event)| event));
        }
        if self.sources.iter().all(|source| source.closed && source.head.is_none()) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    pub(super) async fn recv_many(&mut self, buffer: &mut Vec<FileEvent>, limit: usize) -> usize {
        poll_fn(|cx| {
            let mut received = 0;
            let mut bytes = 0;
            while received < limit && bytes < RECEIVE_BYTES_PER_TURN {
                match self.poll_recv(cx) {
                    Poll::Ready(Some(event)) => {
                        bytes += match &event {
                            FileEvent::OrderStatus(line) | FileEvent::OrderDiff(line) | FileEvent::Fill(line) => {
                                line.len()
                            }
                            FileEvent::ContinuityLost(_) => 0,
                        };
                        buffer.push(event);
                        received += 1;
                    }
                    Poll::Pending if received == 0 => return Poll::Pending,
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(received)
        })
        .await
    }
}

enum FileLineSink {
    Events { source: EventSource, tx: SourceSender },
    FillProgress { recorder: OrderSyncRecorder, event_tx: SourceSender },
}

impl FileLineSink {
    const fn source(&self) -> EventSource {
        match self {
            Self::Events { source, .. } => *source,
            Self::FillProgress { .. } => EventSource::Fills,
        }
    }

    #[cfg(test)]
    fn submit(&self, line: String) -> bool {
        self.submit_lines(vec![line])
    }

    fn submit_lines(&self, lines: Vec<String>) -> bool {
        match self {
            Self::Events { tx, .. } => tx.send_lines(lines),
            Self::FillProgress { recorder, .. } => {
                for line in lines {
                    match recorder.observe_fill_line(&line) {
                        Ok(_) => {
                            FILE_EVENTS_TOTAL.with_label_values(&["fills"]).inc();
                            FILE_LINES_PARSED_TOTAL.with_label_values(&["fills"]).inc_by(line.len() as u64);
                            EVENTS_PROCESSED_TOTAL.with_label_values(&["fills"]).inc();
                        }
                        Err(err) => {
                            PARSE_ERRORS_TOTAL.with_label_values(&["fills"]).inc();
                            let count = ORDER_SYNC_PARSE_ERR_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
                            if count.is_multiple_of(1_000) {
                                error!("Order-sync fill parse error #{count}: {err}");
                            }
                        }
                    }
                }
                true
            }
        }
    }

    fn submit_continuity_loss(&self) -> bool {
        match self {
            Self::Events { tx, .. } => tx.send(SourceMessage::ContinuityLost),
            Self::FillProgress { event_tx, .. } => event_tx.send(SourceMessage::ContinuityLost),
        }
    }

    fn submit_caught_up(&self) -> bool {
        let tx = match self {
            Self::Events { tx, .. } => tx,
            Self::FillProgress { event_tx, .. } => event_tx,
        };
        tx.send(SourceMessage::CaughtUp)
    }
}

fn file_line_sink(
    source: EventSource,
    features: FeatureSet,
    tx: SourceSender,
    order_sync_recorder: Option<OrderSyncRecorder>,
) -> Option<FileLineSink> {
    if source == EventSource::Fills && !features.needs_fill_batches() {
        return order_sync_recorder.map(|recorder| FileLineSink::FillProgress { recorder, event_tx: tx });
    }
    Some(FileLineSink::Events { source, tx })
}

pub(super) fn enabled_event_sources(features: FeatureSet) -> Vec<EventSource> {
    let mut sources = Vec::new();
    if features.watch_order_statuses() {
        sources.push(EventSource::OrderStatuses);
    }
    if features.watch_fills() {
        sources.push(EventSource::Fills);
    }
    if features.watch_order_diffs() {
        sources.push(EventSource::OrderDiffs);
    }
    sources
}

fn submit_file_read(sink: &FileLineSink, read: FileRead) -> bool {
    if read.continuity == FileContinuity::Lost && !sink.submit_continuity_loss() {
        return false;
    }
    sink.submit_lines(read.lines)
}

fn submit_available(sink: &FileLineSink, read: FileRead, caught_up: &mut bool) -> bool {
    let progress = read.progress;
    if !submit_file_read(sink, read) {
        return false;
    }
    match progress {
        ReadProgress::Data | ReadProgress::Retry => *caught_up = false,
        ReadProgress::Idle if !*caught_up => {
            if !sink.submit_caught_up() {
                return false;
            }
            *caught_up = true;
        }
        ReadProgress::Idle => {}
    }
    true
}

const NOTIFY_DATA: u8 = 1;
const NOTIFY_RESCAN: u8 = 2;
const NOTIFY_ERROR: u8 = 4;
const FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);

struct WatcherSignal {
    flags: AtomicU8,
    tx: std::sync::mpsc::SyncSender<()>,
}

impl WatcherSignal {
    fn notify(&self, flags: u8) {
        self.flags.fetch_or(flags, AtomicOrdering::Release);
        let _ = self.tx.try_send(());
    }

    fn take(&self) -> u8 {
        self.flags.swap(0, AtomicOrdering::AcqRel)
    }
}

fn notification_flags(event: &Result<Event, notify::Error>) -> u8 {
    use notify::event::{EventKind, ModifyKind};
    match event {
        Err(_) => NOTIFY_RESCAN | NOTIFY_ERROR,
        Ok(event) if event.need_rescan() => NOTIFY_RESCAN,
        Ok(event) => match event.kind {
            EventKind::Modify(ModifyKind::Data(_)) => NOTIFY_DATA,
            EventKind::Create(_) | EventKind::Remove(_) | EventKind::Modify(_) | EventKind::Any | EventKind::Other => {
                NOTIFY_RESCAN
            }
            EventKind::Access(_) => 0,
        },
    }
}

fn read_available(reader: &mut FileReader, rescan: bool) -> FileRead {
    if rescan && let Some(path) = reader.check_for_newer_file() {
        if reader.is_tracking() {
            return reader.on_create(&path);
        }
        reader.start_tracking(&path);
        info!("Tracking stream: {:?}", reader.current_path());
    }
    reader.read_tracked()
}

fn spawn_file_watcher(dir: PathBuf, sink: FileLineSink) -> thread::JoinHandle<()> {
    let source = sink.source();
    let source_label = super::source_label(source);
    thread::spawn(move || {
        info!("{source} watcher thread started for {}", dir.display());
        let mut reader = FileReader::new(dir.clone());
        let (event_tx, event_rx) = std::sync::mpsc::sync_channel(1);
        let signal = Arc::new(WatcherSignal { flags: AtomicU8::new(0), tx: event_tx });
        let callback_signal = Arc::clone(&signal);
        let mut watcher = match recommended_watcher(move |event: Result<Event, _>| {
            let flags = notification_flags(&event);
            if flags != 0 {
                callback_signal.notify(flags);
            }
        }) {
            Ok(watcher) => watcher,
            Err(err) => {
                error!("{source} watcher failed to create: {err}");
                return;
            }
        };
        if let Err(err) = watcher.watch(&dir, RecursiveMode::Recursive) {
            error!("{source} watcher failed to start: {err}");
            return;
        }

        let read_bytes = FILE_READ_BYTES_TOTAL.with_label_values(&[source_label]);
        let read_calls = FILE_READ_CALLS_TOTAL.with_label_values(&[source_label]);
        let read_duration = FILE_READ_DURATION.with_label_values(&[source_label]);
        let unread_bytes = FILE_UNREAD_BYTES.with_label_values(&[source_label]);
        let notify_wakes = FILE_WATCHER_WAKEUPS_TOTAL.with_label_values(&[source_label, "notify"]);
        let poll_wakes = FILE_WATCHER_WAKEUPS_TOTAL.with_label_values(&[source_label, "poll"]);
        let rescans = FILE_WATCHER_WAKEUPS_TOTAL.with_label_values(&[source_label, "rescan"]);
        let mut next_scan = Instant::now();
        let mut caught_up = false;
        loop {
            let tx = match &sink {
                FileLineSink::Events { tx, .. } => tx,
                FileLineSink::FillProgress { event_tx, .. } => event_tx,
            };
            if tx.tx.is_closed() {
                return;
            }
            let flags = signal.take();
            if flags & NOTIFY_ERROR != 0 {
                error!("{source} watcher reported an error; reconciling stream files");
            }
            let rescan = flags & NOTIFY_RESCAN != 0 || Instant::now() >= next_scan;
            if rescan {
                next_scan = Instant::now() + DISCOVERY_INTERVAL;
                rescans.inc();
            }
            let started = Instant::now();
            let read = read_available(&mut reader, rescan);
            read_duration.observe(started.elapsed().as_secs_f64());
            read_calls.inc();
            read_bytes.inc_by(read.bytes_read as u64);
            unread_bytes.set(i64::try_from(read.unread_bytes).unwrap_or(i64::MAX));
            let progress = read.progress;
            if progress == ReadProgress::Data && read.bytes_read == 0 {
                next_scan = Instant::now();
            }
            if !submit_available(&sink, read, &mut caught_up) {
                return;
            }
            if progress == ReadProgress::Data {
                continue;
            }
            match event_rx.recv_timeout(FALLBACK_POLL_INTERVAL) {
                Ok(()) => notify_wakes.inc(),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => poll_wakes.inc(),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    })
}

pub(super) fn file_event_channels(sources: &[EventSource], capacity: usize) -> (Vec<SourceSender>, FileEventReceiver) {
    let per_source_capacity = (capacity / sources.len().max(1)).max(1);
    let (senders, sources) = sources
        .iter()
        .map(|&source| {
            let (tx, rx) = channel(per_source_capacity);
            let budget = Arc::new(QueueBudget {
                state: Mutex::new(BudgetState::default()),
                space: Condvar::new(),
                limit: QUEUE_BYTES_PER_SOURCE,
                lines: AtomicUsize::new(0),
                source: super::source_label(source),
            });
            (
                SourceSender { tx, budget: Arc::clone(&budget) },
                SourceQueue { source, rx, budget, batch: None, head: None, caught_up: false, closed: false },
            )
        })
        .unzip();
    (senders, FileEventReceiver { sources, next_source: 0 })
}

/// Uses *_streaming directories (for --stream-with-block-info mode)
pub(crate) fn start_parallel_file_watchers(
    data_dir: PathBuf,
    features: FeatureSet,
    order_sync_recorder: Option<OrderSyncRecorder>,
) -> (FileEventReceiver, Vec<thread::JoinHandle<()>>) {
    // Full queues park the readers and leave the remaining file backlog on disk.
    let sources = enabled_event_sources(features);
    let (senders, rx) = file_event_channels(&sources, 256);
    let mut handles = Vec::new();

    for (source, tx) in sources.into_iter().zip(senders) {
        let dir = source.event_source_dir_streaming(&data_dir);
        info!("{source} dir: {}", dir.display());
        let Some(sink) = file_line_sink(source, features, tx.clone(), order_sync_recorder.clone()) else {
            error!("Order-sync fill watcher could not start without a recorder");
            continue;
        };
        handles.push(spawn_file_watcher(dir, sink));
    }

    (rx, handles)
}

#[cfg(test)]
mod tests {
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
        assert_eq!(
            enabled_event_sources(features("allbbo")),
            vec![EventSource::OrderStatuses, EventSource::OrderDiffs]
        );
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
        let sink =
            file_line_sink(EventSource::Fills, features("trades,ordersync"), tx, Some(OrderSyncRecorder::default()))
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
        assert!(
            matches!(rx.try_recv(), Ok(SourceMessage::Lines(batch)) if batch.lines.as_slice() == ["recovered line"])
        );
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
        let line = |height| {
            format!("{{\"block_number\":{height},\"payload\":\"{}\"}}", "x".repeat(RECEIVE_BYTES_PER_TURN / 2))
        };
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
        let watcher = spawn_file_watcher(base_dir.clone(), sink);
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
}
