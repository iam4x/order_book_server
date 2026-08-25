// HFT-optimized parallel file watcher
// Each event source runs on its own thread for maximum throughput

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::{Duration, Instant},
};

use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::{
    FeatureSet,
    metrics::{EVENTS_PROCESSED_TOTAL, FILE_EVENTS_TOTAL, FILE_LINES_PARSED_TOTAL, PARSE_ERRORS_TOTAL},
    order_sync::OrderSyncRecorder,
    types::node_data::EventSource,
};

static ORDER_SYNC_PARSE_ERR_COUNT: AtomicU64 = AtomicU64::new(0);

/// Message sent from file watcher threads to the main processor
#[derive(Debug)]
pub(crate) enum FileEvent {
    OrderStatus(String),
    OrderDiff(String),
    Fill(String),
}

enum FileLineSink {
    Events { source: EventSource, tx: Sender<FileEvent> },
    FillProgress { recorder: OrderSyncRecorder, _event_tx: Sender<FileEvent> },
}

impl FileLineSink {
    const fn source(&self) -> EventSource {
        match self {
            Self::Events { source, .. } => *source,
            Self::FillProgress { .. } => EventSource::Fills,
        }
    }

    fn submit(&self, line: String) -> bool {
        match self {
            Self::Events { source, tx } => {
                let event = match source {
                    EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                    EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                    EventSource::Fills => FileEvent::Fill(line),
                };
                tx.blocking_send(event).is_ok()
            }
            Self::FillProgress { recorder, .. } => {
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
                true
            }
        }
    }
}

fn file_line_sink(
    source: EventSource,
    features: FeatureSet,
    tx: Sender<FileEvent>,
    order_sync_recorder: Option<OrderSyncRecorder>,
) -> Option<FileLineSink> {
    if source == EventSource::Fills && !features.needs_fill_batches() {
        return order_sync_recorder.map(|recorder| FileLineSink::FillProgress { recorder, _event_tx: tx });
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

/// Hard cap on a single un-terminated JSON line. The streaming files write
/// newline-delimited JSON; this bound is a safety net against a corrupt/partial
/// flush from the node that would otherwise let `partial_line` grow without
/// limit and OOM the host.
const MAX_PARTIAL_LINE_BYTES: usize = 16 * 1024 * 1024;

/// File reader state for a single source
struct FileReader {
    current_path: Option<PathBuf>,
    file_position: u64,
    partial_line: String,
    base_dir: PathBuf, // Base streaming directory to scan for new files
}

impl FileReader {
    fn new(base_dir: PathBuf) -> Self {
        Self { current_path: None, file_position: 0, partial_line: String::new(), base_dir }
    }

    /// Find the latest file in the streaming directory tree
    /// Scans hourly/YYYYMMDD/HH structure and returns the most recently modified file
    fn find_latest_file(&self) -> Option<PathBuf> {
        let hourly_dir = self.base_dir.join("hourly");
        if !hourly_dir.exists() {
            return None;
        }

        // Find the latest day directory
        let mut latest_day: Option<PathBuf> = None;
        if let Ok(entries) = std::fs::read_dir(&hourly_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if latest_day.is_none() || path > latest_day.clone().unwrap() {
                        latest_day = Some(path);
                    }
                }
            }
        }

        let day_dir = latest_day?;

        // Find the latest hour file in this day
        let mut latest_file: Option<PathBuf> = None;
        let mut latest_mtime: Option<std::time::SystemTime> = None;

        if let Ok(entries) = std::fs::read_dir(&day_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = path.metadata() {
                        if let Ok(mtime) = metadata.modified() {
                            if latest_mtime.is_none() || mtime > latest_mtime.unwrap() {
                                latest_mtime = Some(mtime);
                                latest_file = Some(path);
                            }
                        }
                    }
                }
            }
        }

        latest_file
    }

    /// Check if there's a newer file than what we're currently tracking
    fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        if let Some(latest) = self.find_latest_file() {
            if let Some(ref current) = self.current_path {
                if latest != *current {
                    // Check if the new file has data (modification time is newer)
                    if let (Ok(latest_meta), Ok(current_meta)) = (latest.metadata(), current.metadata()) {
                        if let (Ok(latest_mtime), Ok(current_mtime)) = (latest_meta.modified(), current_meta.modified())
                        {
                            if latest_mtime > current_mtime {
                                return Some(latest);
                            }
                        }
                    }
                }
            } else {
                // No current file, use the latest
                return Some(latest);
            }
        }
        None
    }

    /// Process file modification - read new data and return lines
    fn on_modify(&mut self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(ref path) = self.current_path {
            // Open file, seek to last position, read new data
            if let Ok(mut file) = File::open(path) {
                // Get fresh file size from opened handle
                if let Ok(metadata) = file.metadata() {
                    let file_size = metadata.len();

                    // Only read if there's new data
                    if file_size > self.file_position {
                        if file.seek(SeekFrom::Start(self.file_position)).is_ok() {
                            let mut buf = String::new();
                            match file.read_to_string(&mut buf) {
                                Ok(bytes_read) => {
                                    if bytes_read > 0 {
                                        // Update position
                                        self.file_position += bytes_read as u64;

                                        // Prepend any partial line from last read
                                        let full_buf = std::mem::take(&mut self.partial_line) + &buf;

                                        let mut line_iter = full_buf.lines().peekable();

                                        while let Some(line) = line_iter.next() {
                                            if line_iter.peek().is_some() {
                                                // Not the last line
                                                if !line.is_empty() {
                                                    // Validate JSON structure - must start with { and end with }
                                                    if line.starts_with('{') && line.ends_with('}') {
                                                        lines.push(line.to_string());
                                                    } else {
                                                        // Incomplete JSON - store for next read
                                                        self.partial_line = line.to_string();
                                                    }
                                                }
                                            } else {
                                                // Last line - might be partial
                                                if buf.ends_with('\n') && !line.is_empty() {
                                                    // Validate JSON structure
                                                    if line.starts_with('{') && line.ends_with('}') {
                                                        lines.push(line.to_string());
                                                    } else {
                                                        // Incomplete JSON - store for next read
                                                        self.partial_line = line.to_string();
                                                    }
                                                } else if !line.is_empty() {
                                                    // Partial line without newline
                                                    self.partial_line = line.to_string();
                                                }
                                            }
                                        }

                                        // Bound the partial-line buffer. If the upstream goes wedged
                                        // mid-JSON (corrupt flush, mmap weirdness, multi-MB single line),
                                        // we'd otherwise grow `partial_line` until we OOM. Drop and resync
                                        // on the next newline.
                                        if self.partial_line.len() > MAX_PARTIAL_LINE_BYTES {
                                            error!(
                                                "partial_line exceeded {} bytes ({} bytes buffered); discarding and resyncing",
                                                MAX_PARTIAL_LINE_BYTES,
                                                self.partial_line.len()
                                            );
                                            self.partial_line.clear();
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("Read error: {}", err);
                                }
                            }
                        }
                    }
                }
            }
        }
        lines
    }

    /// Switch to a new file (on create event)
    fn on_create(&mut self, path: &PathBuf) -> Vec<String> {
        // First, read remaining data from old file
        let old_lines = self.on_modify();

        // Start tracking new file from beginning
        self.current_path = Some(path.clone());
        self.file_position = 0;
        self.partial_line.clear();

        old_lines
    }

    /// Track an existing file (first event we see for it)
    fn start_tracking(&mut self, path: &PathBuf) {
        // Get current file size to start from end
        if let Ok(metadata) = std::fs::metadata(path) {
            self.file_position = metadata.len();
        } else {
            self.file_position = 0;
        }
        self.current_path = Some(path.clone());
        self.partial_line.clear();
    }
}

/// Spawn a file watcher thread for a single event source
/// Uses polling with inotify hints for streaming files
fn spawn_file_watcher(dir: PathBuf, sink: FileLineSink, last_event: Arc<AtomicU64>) -> thread::JoinHandle<()> {
    let source = sink.source();
    let source_name = match source {
        EventSource::OrderStatuses => "OrderStatuses",
        EventSource::Fills => "Fills",
        EventSource::OrderDiffs => "OrderDiffs",
    };

    thread::spawn(move || {
        info!("{} watcher thread started for {:?}", source_name, dir);

        let mut reader = FileReader::new(dir.clone());

        // Create watcher with callback
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut watcher = match recommended_watcher(move |res: Result<Event, _>| {
            drop(event_tx.send(res));
        }) {
            Ok(w) => w,
            Err(err) => {
                error!("{} watcher failed to create: {}", source_name, err);
                return;
            }
        };

        if let Err(err) = watcher.watch(&dir, RecursiveMode::Recursive) {
            error!("{} watcher failed to start: {}", source_name, err);
            return;
        }

        // HFT CRITICAL: Use fast polling (1ms) for lowest latency
        // inotify provides immediate notifications when available, but polling ensures we never wait
        let poll_interval = Duration::from_millis(1);

        // Main event loop - primarily event-driven with fallback polling
        let mut poll_count = 0u64;
        loop {
            poll_count += 1;

            // Wait for inotify events (with fallback timeout)
            match event_rx.recv_timeout(poll_interval) {
                Ok(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let path = &event.paths[0];
                        if path.is_dir() {
                            continue;
                        }

                        if event.kind.is_create() {
                            info!("{} new file: {:?}", source_name, path.file_name());
                            let old_lines = reader.on_create(path);
                            for line in old_lines {
                                if !sink.submit(line) {
                                    error!("{} channel closed, exiting", source_name);
                                    return;
                                }
                            }
                        } else if reader.current_path.is_none() {
                            // First time seeing this file
                            info!("{} tracking: {:?}", source_name, path.file_name());
                            reader.start_tracking(path);
                        }

                        // EVENT-DRIVEN: Read data when inotify fires modify event
                        let lines = reader.on_modify();
                        for line in lines {
                            if !sink.submit(line) {
                                error!("{} channel closed, exiting", source_name);
                                return;
                            }

                            // Update health timestamp
                            last_event.store(Instant::now().elapsed().as_millis() as u64, AtomicOrdering::Relaxed);
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("{} watcher error: {}", source_name, err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Fallback polling - safety net for missed events
                    // This runs every 500ms instead of every 10ms
                    let lines = reader.on_modify();
                    for line in lines {
                        if !sink.submit(line) {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }

                        last_event.store(Instant::now().elapsed().as_millis() as u64, AtomicOrdering::Relaxed);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    error!("{} event channel closed, exiting", source_name);
                    return;
                }
            }

            // Every 10000 polls (~10 seconds), check for newer files (handles day rotation)
            if poll_count % 10_000 == 0 {
                if let Some(newer_file) = reader.check_for_newer_file() {
                    info!("{} detected newer file (day rotation?): {:?}", source_name, newer_file.file_name());
                    // Switch to the new file
                    let old_lines = reader.on_create(&newer_file);
                    for line in old_lines {
                        if !sink.submit(line) {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Uses *_streaming directories (for --stream-with-block-info mode)
pub(crate) fn start_parallel_file_watchers(
    data_dir: PathBuf,
    features: FeatureSet,
    order_sync_recorder: Option<OrderSyncRecorder>,
) -> (Receiver<FileEvent>, Vec<thread::JoinHandle<()>>, Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>) {
    // The bound caps the in-memory backlog. When full, blocking_send parks the
    // file readers and leaves unread events on disk.
    let (tx, rx) = channel(10_000);
    let mut handles = Vec::new();

    let last_order_status = Arc::new(AtomicU64::new(0));
    let last_fills = Arc::new(AtomicU64::new(0));
    let last_order_diffs = Arc::new(AtomicU64::new(0));

    // HFT mode uses streaming directories (for --stream-with-block-info)
    for source in enabled_event_sources(features) {
        let dir = source.event_source_dir_streaming(&data_dir);
        info!("{} dir: {:?}", source, dir);
        let last_event = match source {
            EventSource::OrderStatuses => last_order_status.clone(),
            EventSource::Fills => last_fills.clone(),
            EventSource::OrderDiffs => last_order_diffs.clone(),
        };
        let Some(sink) = file_line_sink(source, features, tx.clone(), order_sync_recorder.clone()) else {
            error!("Order-sync fill watcher could not start without a recorder");
            continue;
        };
        handles.push(spawn_file_watcher(dir, sink, last_event));
    }

    (rx, handles, last_order_status, last_fills, last_order_diffs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn features(value: &str) -> FeatureSet {
        value.parse().expect("valid features")
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
        let (tx, mut rx) = channel(1);
        let recorder = OrderSyncRecorder::default();
        let sink = file_line_sink(EventSource::Fills, features("bbo,ordersync"), tx, Some(recorder.clone()))
            .expect("ordersync recorder creates a fill sink");

        assert!(sink.submit(r#"{"events":[[null,{"time":300000}]]}"#.to_string()));
        assert_eq!(recorder.status_at(600_000).last_order_at, Some(300));
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
    }

    #[test]
    fn trades_and_ordersync_keep_the_existing_full_fill_path() {
        let (tx, mut rx) = channel(1);
        let sink =
            file_line_sink(EventSource::Fills, features("trades,ordersync"), tx, Some(OrderSyncRecorder::default()))
                .expect("fill sink exists");

        assert!(sink.submit("fill line".to_string()));
        assert!(matches!(rx.try_recv(), Ok(FileEvent::Fill(line)) if line == "fill line"));
    }
}
