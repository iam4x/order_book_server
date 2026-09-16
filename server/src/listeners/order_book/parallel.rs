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
/// Start each block with statuses, then alternate book sources within that block.
pub(crate) struct FileEventReceiver {
    sources: Vec<SourceQueue>,
    last_book_event: Option<(EventSource, u64)>,
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
        let selected = self
            .sources
            .iter()
            .enumerate()
            .filter(|(_, source)| !waiting_for_book || source.source == EventSource::Fills)
            .filter_map(|(index, source)| {
                let height = source.head.as_ref()?.0;
                let preferred = if self.last_book_event == Some((EventSource::OrderStatuses, height)) {
                    EventSource::OrderDiffs
                } else {
                    EventSource::OrderStatuses
                };
                let priority =
                    if source.source == EventSource::Fills { 2 } else { u8::from(source.source != preferred) };
                Some((index, height, priority))
            })
            .min_by_key(|&(_, height, priority)| (height, priority));
        if let Some((index, height, _)) = selected {
            let source = &mut self.sources[index];
            if source.source != EventSource::Fills {
                self.last_book_event = Some((source.source, height));
            }
            return Poll::Ready(source.head.take().map(|(_, event)| event));
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
        if self.flags.fetch_or(flags, AtomicOrdering::Release) == 0 {
            let _ = self.tx.try_send(());
        }
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
        return reader.on_create(&path);
    }
    reader.read_tracked()
}

fn spawn_file_watcher(
    dir: PathBuf,
    sink: FileLineSink,
) -> (thread::JoinHandle<()>, tokio::sync::oneshot::Receiver<()>) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let source = sink.source();
    let source_label = super::source_label(source);
    let handle = thread::spawn(move || {
        info!("{source} watcher thread started for {}", dir.display());
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

        let mut reader = match FileReader::attach(dir) {
            Ok(reader) => reader,
            Err(err) => {
                error!("{source} watcher failed to attach: {err}");
                return;
            }
        };
        info!("{source} tracking: {:?}", reader.current_path());
        if ready_tx.send(()).is_err() {
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
    });
    (handle, ready_rx)
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
    (senders, FileEventReceiver { sources, last_book_event: None })
}

/// Uses *_streaming directories (for --stream-with-block-info mode)
pub(crate) async fn start_parallel_file_watchers(
    data_dir: PathBuf,
    features: FeatureSet,
    order_sync_recorder: Option<OrderSyncRecorder>,
) -> crate::prelude::Result<(FileEventReceiver, Vec<thread::JoinHandle<()>>)> {
    // Full queues park the readers and leave the remaining file backlog on disk.
    let sources = enabled_event_sources(features);
    let (senders, rx) = file_event_channels(&sources, 256);
    let mut handles = Vec::new();
    let mut pending = Vec::new();

    for (source, tx) in sources.into_iter().zip(senders) {
        let dir = source.event_source_dir_streaming(&data_dir);
        info!("{source} dir: {}", dir.display());
        let sink = file_line_sink(source, features, tx, order_sync_recorder.clone())
            .ok_or("Order-sync fill watcher could not start without a recorder")?;
        let (handle, ready) = spawn_file_watcher(dir, sink);
        handles.push(handle);
        pending.push(async move {
            ready.await.map_err(|_| -> crate::prelude::Error {
                format!("{source} watcher failed before stream attachment").into()
            })
        });
    }

    futures_util::future::try_join_all(pending).await?;
    Ok((rx, handles))
}

#[cfg(test)]
mod tests;
