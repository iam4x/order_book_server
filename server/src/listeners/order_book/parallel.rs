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
    ContinuityLost(EventSource),
}

enum FileLineSink {
    Events { source: EventSource, tx: Sender<FileEvent> },
    FillProgress { recorder: OrderSyncRecorder, event_tx: Sender<FileEvent> },
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

    fn submit_continuity_loss(&self) -> bool {
        match self {
            Self::Events { source, tx } => tx.blocking_send(FileEvent::ContinuityLost(*source)).is_ok(),
            Self::FillProgress { event_tx, .. } => {
                event_tx.blocking_send(FileEvent::ContinuityLost(EventSource::Fills)).is_ok()
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

/// Hard cap on a single un-terminated JSON line. The streaming files write
/// newline-delimited JSON; this bound is a safety net against a corrupt/partial
/// flush from the node that would otherwise let `partial_line` grow without
/// limit and OOM the host.
const MAX_PARTIAL_LINE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileContinuity {
    Preserved,
    Lost,
}

#[derive(Debug, PartialEq, Eq)]
struct FileRead {
    lines: Vec<String>,
    continuity: FileContinuity,
}

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
        let latest = self.find_latest_file()?;
        let Some(current) = self.current_path.as_ref() else {
            return Some(latest);
        };
        if latest == *current {
            return None;
        }

        let current_metadata = match current.metadata() {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Some(latest),
            Err(err) => {
                error!("Failed to inspect tracked file {:?}: {}; retrying", current, err);
                return None;
            }
        };
        if !current_metadata.is_file() {
            return Some(latest);
        }

        let latest_metadata = latest.metadata().ok()?;
        if !latest_metadata.is_file() {
            return None;
        }
        let latest_mtime = latest_metadata.modified().ok()?;
        let current_mtime = current_metadata.modified().ok()?;
        (latest_mtime > current_mtime).then_some(latest)
    }

    /// Process file modification - read new data and return lines
    fn on_modify(&mut self) -> FileRead {
        let mut lines = Vec::new();
        let mut continuity = FileContinuity::Preserved;
        if let Some(ref path) = self.current_path {
            // Open file, seek to last position, read new data
            if let Ok(mut file) = File::open(path) {
                // Get fresh file size from opened handle
                if let Ok(metadata) = file.metadata() {
                    let file_size = metadata.len();

                    if file_size < self.file_position {
                        self.file_position = 0;
                        self.partial_line.clear();
                        continuity = FileContinuity::Lost;
                    }

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
        FileRead { lines, continuity }
    }

    /// Switch to a new file (on create event)
    fn on_create(&mut self, path: &PathBuf) -> FileRead {
        let candidate_metadata = match path.metadata() {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return FileRead { lines: Vec::new(), continuity: FileContinuity::Preserved };
            }
            Err(err) => {
                error!("Failed to inspect created path {:?}: {}; retrying", path, err);
                return FileRead { lines: Vec::new(), continuity: FileContinuity::Preserved };
            }
        };
        if !candidate_metadata.is_file() || self.current_path.as_ref() == Some(path) {
            return FileRead { lines: Vec::new(), continuity: FileContinuity::Preserved };
        }

        let current_continuity = match self.current_path.as_ref() {
            None => FileContinuity::Preserved,
            Some(current) => match current.metadata() {
                Ok(metadata) if metadata.is_file() => FileContinuity::Preserved,
                Ok(_) => FileContinuity::Lost,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => FileContinuity::Lost,
                Err(err) => {
                    error!("Failed to inspect tracked file {:?} before switching: {}; retrying", current, err);
                    return FileRead { lines: Vec::new(), continuity: FileContinuity::Preserved };
                }
            },
        };

        let mut old_read = match current_continuity {
            FileContinuity::Preserved => self.on_modify(),
            FileContinuity::Lost => FileRead { lines: Vec::new(), continuity: FileContinuity::Lost },
        };

        // Start tracking new file from beginning
        self.current_path = Some(path.clone());
        self.file_position = 0;
        self.partial_line.clear();

        if current_continuity == FileContinuity::Lost {
            old_read.continuity = FileContinuity::Lost;
        }
        old_read
    }

    /// Track an existing file (first event we see for it)
    fn start_tracking(&mut self, path: &PathBuf) {
        // Get current file size to start from end
        let Ok(metadata) = std::fs::metadata(path) else {
            return;
        };
        if !metadata.is_file() {
            return;
        }

        self.file_position = metadata.len();
        self.current_path = Some(path.clone());
        self.partial_line.clear();
    }
}

fn submit_file_read(sink: &FileLineSink, read: FileRead, last_event: Option<&AtomicU64>) -> bool {
    if read.continuity == FileContinuity::Lost && !sink.submit_continuity_loss() {
        return false;
    }
    for line in read.lines {
        if !sink.submit(line) {
            return false;
        }
        if let Some(last_event) = last_event {
            last_event.store(Instant::now().elapsed().as_millis() as u64, AtomicOrdering::Relaxed);
        }
    }
    true
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
                            if !submit_file_read(&sink, reader.on_create(path), None) {
                                error!("{} channel closed, exiting", source_name);
                                return;
                            }
                        } else if reader.current_path.is_none() {
                            // First time seeing this file
                            info!("{} tracking: {:?}", source_name, path.file_name());
                            reader.start_tracking(path);
                        }

                        // EVENT-DRIVEN: Read data when inotify fires modify event
                        if !submit_file_read(&sink, reader.on_modify(), Some(last_event.as_ref())) {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("{} watcher error: {}", source_name, err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Fallback polling - safety net for missed events
                    // This runs every 500ms instead of every 10ms
                    if !submit_file_read(&sink, reader.on_modify(), Some(last_event.as_ref())) {
                        error!("{} channel closed, exiting", source_name);
                        return;
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
                    if !submit_file_read(&sink, reader.on_create(&newer_file), None) {
                        error!("{} channel closed, exiting", source_name);
                        return;
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

    #[test]
    fn continuity_loss_precedes_recovered_lines_on_the_shared_channel() {
        let (tx, mut rx) = channel(2);
        let sink = file_line_sink(EventSource::OrderDiffs, features("bbo"), tx, None).expect("diff sink exists");

        assert!(sink.submit_continuity_loss());
        assert!(sink.submit("recovered line".to_string()));

        assert!(matches!(rx.try_recv(), Ok(FileEvent::ContinuityLost(EventSource::OrderDiffs))));
        assert!(matches!(rx.try_recv(), Ok(FileEvent::OrderDiff(line)) if line == "recovered line"));
    }

    #[test]
    fn create_event_for_a_day_directory_does_not_replace_the_tracked_file() {
        let base_dir = stream_test_dir("ignore-day-directory");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        reader.on_create(&day_dir);

        assert_eq!(reader.current_path.as_deref(), Some(hour_file.as_path()));
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn polling_recovers_when_the_tracked_file_disappears() {
        let base_dir = stream_test_dir("recover-missing-file");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let old_file = day_dir.join("5");
        std::fs::write(&old_file, "").expect("old hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        std::fs::remove_file(&old_file).expect("old hour file should be removed");
        let current_file = day_dir.join("6");
        std::fs::write(&current_file, "{}\n").expect("current hour file should exist");

        let recovered_file = reader.check_for_newer_file().expect("polling should recover the current hour file");
        assert_eq!(recovered_file, current_file);
        assert!(reader.on_create(&recovered_file).lines.is_empty());
        assert_eq!(reader.on_modify().lines, vec!["{}"]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn start_tracking_ignores_missing_and_non_file_candidates() {
        let base_dir = stream_test_dir("ignore-invalid-start");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "{\"sequence\":1}\n").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        let file_position = reader.file_position;

        reader.start_tracking(&day_dir);
        reader.start_tracking(&day_dir.join("missing"));

        assert_eq!(reader.current_path.as_deref(), Some(hour_file.as_path()));
        assert_eq!(reader.file_position, file_position);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn duplicate_create_for_the_current_file_preserves_the_offset_without_replay() {
        let base_dir = stream_test_dir("duplicate-create");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "{\"sequence\":1}\n").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        let file_position = reader.file_position;

        assert!(reader.on_create(&hour_file).lines.is_empty());
        assert!(reader.on_create(&hour_file).lines.is_empty());
        assert_eq!(reader.file_position, file_position);

        append_to_file(&hour_file, "{\"sequence\":2}\n");
        assert_eq!(reader.on_modify().lines, vec![r#"{"sequence":2}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn same_path_shorter_recreation_rewinds_to_byte_zero() {
        let base_dir = stream_test_dir("same-path-shorter");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "{\"sequence\":123456789}\n").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        append_to_file(&hour_file, "{\"stale\":");
        assert!(reader.on_modify().lines.is_empty());
        assert_eq!(reader.partial_line, "{\"stale\":");
        let file_position = reader.file_position;
        std::fs::write(&hour_file, "{}\n").expect("hour file should be recreated");

        assert!(reader.on_create(&hour_file).lines.is_empty());
        assert_eq!(reader.file_position, file_position);
        assert_eq!(reader.on_modify().lines, vec!["{}"]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn first_modify_attaches_at_eof_without_replaying_existing_lines() {
        let base_dir = stream_test_dir("first-modify-eof");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "{\"sequence\":1}\n").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);

        assert!(reader.on_modify().lines.is_empty());
        append_to_file(&hour_file, "{\"sequence\":2}\n");
        assert_eq!(reader.on_modify().lines, vec![r#"{"sequence":2}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn missing_file_recovery_reports_loss_before_replacement_lines() {
        let base_dir = stream_test_dir("missing-file-continuity");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let old_file = day_dir.join("5");
        std::fs::write(&old_file, "").expect("old hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        std::fs::remove_file(&old_file).expect("old hour file should be removed");
        let replacement_file = day_dir.join("6");
        std::fs::write(&replacement_file, "{\"sequence\":1}\n").expect("replacement hour file should exist");

        let recovered_file = reader.check_for_newer_file().expect("polling should find the replacement file");
        let FileRead { lines: old_lines, continuity: switch_continuity } = reader.on_create(&recovered_file);
        let FileRead { lines: replacement_lines, continuity: read_continuity } = reader.on_modify();

        assert!(matches!(switch_continuity, FileContinuity::Lost));
        assert!(old_lines.is_empty());
        assert!(matches!(read_continuity, FileContinuity::Preserved));
        assert_eq!(replacement_lines, vec![r#"{"sequence":1}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn same_path_shrink_reports_continuity_loss() {
        let base_dir = stream_test_dir("shrink-continuity");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "{\"sequence\":123456789}\n").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        std::fs::write(&hour_file, "{}\n").expect("hour file should shrink");

        let FileRead { lines, continuity } = reader.on_modify();

        assert!(matches!(continuity, FileContinuity::Lost));
        assert_eq!(lines, vec!["{}"]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn equal_size_same_path_replacement_reports_loss_and_restarts_at_byte_zero() {
        let base_dir = stream_test_dir("same-path-equal-size-replacement");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        let original = "{\"old\":1}\n";
        let replacement = "{\"new\":2}\n";
        assert_eq!(original.len(), replacement.len());
        std::fs::write(&hour_file, original).expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        let replacement_file = day_dir.join("replacement");
        std::fs::write(&replacement_file, replacement).expect("replacement should exist");
        std::fs::rename(&replacement_file, &hour_file).expect("replacement should install atomically");

        let switched = reader.on_create(&hour_file);
        let read = reader.on_modify();

        assert_eq!(
            (switched.continuity, switched.lines, read.continuity, read.lines),
            (FileContinuity::Lost, Vec::<String>::new(), FileContinuity::Preserved, vec![r#"{"new":2}"#.to_string()],)
        );
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn larger_same_path_replacement_reports_loss_and_restarts_at_byte_zero() {
        let base_dir = stream_test_dir("same-path-larger-replacement");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let hour_file = day_dir.join("6");
        std::fs::write(&hour_file, "{}\n").expect("hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&hour_file);
        let replacement_file = day_dir.join("replacement");
        std::fs::write(&replacement_file, "{\"replacement\":true}\n").expect("replacement should exist");
        std::fs::rename(&replacement_file, &hour_file).expect("replacement should install atomically");

        let switched = reader.on_create(&hour_file);
        let read = reader.on_modify();

        assert_eq!(
            (switched.continuity, switched.lines, read.continuity, read.lines),
            (
                FileContinuity::Lost,
                Vec::<String>::new(),
                FileContinuity::Preserved,
                vec![r#"{"replacement":true}"#.to_string()],
            )
        );
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn skipped_hour_rotation_reports_continuity_loss() {
        let base_dir = stream_test_dir("skipped-hour");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let old_file = day_dir.join("5");
        let new_file = day_dir.join("7");
        std::fs::write(&old_file, "").expect("old hour file should exist");
        std::fs::write(&new_file, "{\"sequence\":7}\n").expect("new hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        let switched = reader.on_create(&new_file);
        let read = reader.on_modify();

        assert_eq!(switched.continuity, FileContinuity::Lost);
        assert_eq!(read.lines, vec![r#"{"sequence":7}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn rotation_with_a_trailing_partial_record_reports_continuity_loss() {
        let base_dir = stream_test_dir("partial-record-rotation");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let old_file = day_dir.join("5");
        let new_file = day_dir.join("6");
        std::fs::write(&old_file, "").expect("old hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        append_to_file(&old_file, "{\"sequence\":");
        std::fs::write(&new_file, "{\"sequence\":6}\n").expect("new hour file should exist");

        let switched = reader.on_create(&new_file);
        let read = reader.on_modify();

        assert_eq!(switched.continuity, FileContinuity::Lost);
        assert_eq!(read.lines, vec![r#"{"sequence":6}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn unlinked_predecessor_is_drained_before_immediate_rotation() {
        let base_dir = stream_test_dir("unlinked-predecessor");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let old_file = day_dir.join("5");
        let new_file = day_dir.join("6");
        std::fs::write(&old_file, "").expect("old hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        append_to_file(&old_file, "{\"sequence\":5}\n");
        std::fs::remove_file(&old_file).expect("old path should be unlinked");
        std::fs::write(&new_file, "{\"sequence\":6}\n").expect("new hour file should exist");

        let switched = reader.on_create(&new_file);
        let read = reader.on_modify();

        assert_eq!(switched.continuity, FileContinuity::Preserved);
        assert_eq!(switched.lines, vec![r#"{"sequence":5}"#]);
        assert_eq!(read.lines, vec![r#"{"sequence":6}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn day_rollover_from_hour_23_to_hour_0_preserves_continuity() {
        let base_dir = stream_test_dir("day-rollover");
        let old_day = base_dir.join("hourly/20260826");
        let new_day = base_dir.join("hourly/20260827");
        std::fs::create_dir_all(&old_day).expect("old day directory should exist");
        std::fs::create_dir_all(&new_day).expect("new day directory should exist");
        let old_file = old_day.join("23");
        let new_file = new_day.join("0");
        std::fs::write(&old_file, "").expect("old hour file should exist");
        std::fs::write(&new_file, "{\"sequence\":0}\n").expect("new hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        let switched = reader.on_create(&new_file);
        let read = reader.on_modify();

        assert_eq!(switched.continuity, FileContinuity::Preserved);
        assert_eq!(read.lines, vec![r#"{"sequence":0}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn delayed_create_for_an_older_hour_is_ignored() {
        let base_dir = stream_test_dir("delayed-older-create");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let older_file = day_dir.join("5");
        let current_file = day_dir.join("6");
        std::fs::write(&older_file, "{\"sequence\":5}\n").expect("older hour file should exist");
        std::fs::write(&current_file, "{\"sequence\":6}\n").expect("current hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&current_file);
        let delayed = reader.on_create(&older_file);

        assert_eq!(delayed.continuity, FileContinuity::Preserved);
        assert!(delayed.lines.is_empty());
        assert_eq!(reader.current_path.as_deref(), Some(current_file.as_path()));
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }

    #[test]
    fn ordinary_rotation_preserves_continuity() {
        let base_dir = stream_test_dir("rotation-continuity");
        let day_dir = base_dir.join("hourly/20260826");
        std::fs::create_dir_all(&day_dir).expect("day directory should exist");
        let old_file = day_dir.join("5");
        std::fs::write(&old_file, "").expect("old hour file should exist");

        let mut reader = FileReader::new(base_dir.clone());
        reader.start_tracking(&old_file);
        append_to_file(&old_file, "{\"sequence\":1}\n");
        let new_file = day_dir.join("6");
        std::fs::write(&new_file, "{\"sequence\":2}\n").expect("new hour file should exist");

        let FileRead { lines: old_lines, continuity: switch_continuity } = reader.on_create(&new_file);
        let FileRead { lines: new_lines, continuity: read_continuity } = reader.on_modify();

        assert!(matches!(switch_continuity, FileContinuity::Preserved));
        assert_eq!(old_lines, vec![r#"{"sequence":1}"#]);
        assert!(matches!(read_continuity, FileContinuity::Preserved));
        assert_eq!(new_lines, vec![r#"{"sequence":2}"#]);
        std::fs::remove_dir_all(base_dir).expect("test stream directory should be removed");
    }
}
