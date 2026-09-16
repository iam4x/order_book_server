use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use log::error;

pub(super) const READ_CHUNK_BYTES: usize = 256 * 1024;
const MAX_PARTIAL_LINE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileContinuity {
    Preserved,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadProgress {
    Data,
    Idle,
    Retry,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct FileRead {
    pub(super) lines: Vec<String>,
    pub(super) continuity: FileContinuity,
    pub(super) bytes_read: usize,
    pub(super) unread_bytes: u64,
    pub(super) progress: ReadProgress,
}

impl FileRead {
    const fn idle() -> Self {
        Self {
            lines: Vec::new(),
            continuity: FileContinuity::Preserved,
            bytes_read: 0,
            unread_bytes: 0,
            progress: ReadProgress::Idle,
        }
    }

    fn data() -> Self {
        Self { progress: ReadProgress::Data, ..Self::idle() }
    }

    fn retry(continuity: FileContinuity) -> Self {
        Self { continuity, progress: ReadProgress::Retry, ..Self::idle() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StreamKey {
    day: NaiveDate,
    hour: u8,
}

impl StreamKey {
    fn from_path(path: &Path) -> Option<Self> {
        let hour = path.file_name()?.to_str()?.parse::<u8>().ok()?;
        if hour > 23 {
            return None;
        }
        let day = NaiveDate::parse_from_str(path.parent()?.file_name()?.to_str()?, "%Y%m%d").ok()?;
        Some(Self { day, hour })
    }

    fn is_immediate_successor_of(self, previous: Self) -> bool {
        if previous.hour < 23 {
            self.day == previous.day && self.hour == previous.hour + 1
        } else {
            self.hour == 0 && previous.day.succ_opt() == Some(self.day)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self { device: metadata.dev(), inode: metadata.ino() }
    }
}

struct OpenedStream {
    path: PathBuf,
    key: StreamKey,
    file: File,
    identity: FileIdentity,
}

impl OpenedStream {
    fn open(path: &Path) -> std::io::Result<Option<Self>> {
        let Some(key) = StreamKey::from_path(path) else {
            return Ok(None);
        };
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Ok(None);
        }
        Ok(Some(Self { path: path.to_path_buf(), key, file, identity: FileIdentity::from_metadata(&metadata) }))
    }
}

struct TrackedStream {
    opened: OpenedStream,
    offset: u64,
    partial_line: Vec<u8>,
    discarding_oversize: bool,
    path_missing: bool,
    read_unhealthy: bool,
    read_failure_reported: bool,
}

impl TrackedStream {
    const fn new(opened: OpenedStream, offset: u64) -> Self {
        Self {
            opened,
            offset,
            partial_line: Vec::new(),
            discarding_oversize: false,
            path_missing: false,
            read_unhealthy: false,
            read_failure_reported: false,
        }
    }

    fn report_read_failure(&mut self) -> FileRead {
        self.read_unhealthy = true;
        let continuity = if self.read_failure_reported {
            FileContinuity::Preserved
        } else {
            self.read_failure_reported = true;
            FileContinuity::Lost
        };
        FileRead::retry(continuity)
    }

    const fn mark_read_healthy(&mut self) {
        self.read_unhealthy = false;
        self.read_failure_reported = false;
    }

    fn extend_partial(&mut self, bytes: &[u8]) {
        let required = self.partial_line.len() + bytes.len();
        debug_assert!(required <= MAX_PARTIAL_LINE_BYTES);
        if self.partial_line.capacity() < required {
            let capacity = self
                .partial_line
                .capacity()
                .max(READ_CHUNK_BYTES)
                .saturating_mul(2)
                .max(required)
                .min(MAX_PARTIAL_LINE_BYTES);
            self.partial_line.reserve_exact(capacity - self.partial_line.len());
        }
        self.partial_line.extend_from_slice(bytes);
    }

    fn frame(&mut self, bytes: &[u8]) -> (Vec<String>, FileContinuity) {
        let mut lines = Vec::new();
        let mut continuity = FileContinuity::Preserved;
        let mut cursor = 0;
        while cursor < bytes.len() {
            if self.discarding_oversize {
                let Some(relative_newline) = bytes[cursor..].iter().position(|&byte| byte == b'\n') else {
                    break;
                };
                cursor += relative_newline + 1;
                self.discarding_oversize = false;
                continue;
            }

            let newline = bytes[cursor..].iter().position(|&byte| byte == b'\n');
            let end = newline.map_or(bytes.len(), |relative| cursor + relative);
            let segment = &bytes[cursor..end];
            if self.partial_line.len().saturating_add(segment.len()) > MAX_PARTIAL_LINE_BYTES {
                self.partial_line.clear();
                continuity = FileContinuity::Lost;
                if newline.is_none() {
                    self.discarding_oversize = true;
                    break;
                }
            } else if newline.is_some() {
                self.finish_line(segment, &mut lines, &mut continuity);
            } else {
                self.extend_partial(segment);
            }

            if newline.is_some() {
                cursor = end + 1;
            } else {
                break;
            }
        }
        (lines, continuity)
    }

    fn finish_line(&mut self, segment: &[u8], lines: &mut Vec<String>, continuity: &mut FileContinuity) {
        if self.partial_line.is_empty() {
            let segment = segment.strip_suffix(b"\r").unwrap_or(segment);
            if !segment.is_empty() {
                match std::str::from_utf8(segment) {
                    Ok(line) => lines.push(line.to_owned()),
                    Err(_) => *continuity = FileContinuity::Lost,
                }
            }
            return;
        }

        self.extend_partial(segment);
        if self.partial_line.last() == Some(&b'\r') {
            self.partial_line.pop();
        }
        if self.partial_line.is_empty() {
            return;
        }
        match String::from_utf8(std::mem::take(&mut self.partial_line)) {
            Ok(mut line) => {
                line.shrink_to_fit();
                lines.push(line);
            }
            Err(_) => *continuity = FileContinuity::Lost,
        }
    }
}

pub(super) struct FileReader {
    tracked: Option<TrackedStream>,
    pending: Option<OpenedStream>,
    base_dir: PathBuf,
    read_buf: Vec<u8>,
}

impl FileReader {
    pub(super) fn new(base_dir: PathBuf) -> Self {
        Self { tracked: None, pending: None, base_dir, read_buf: vec![0; READ_CHUNK_BYTES] }
    }

    pub(super) fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        let Some(current_path) = self.tracked.as_ref().map(|tracked| tracked.opened.path.clone()) else {
            return self.latest_initial_file();
        };
        match OpenedStream::open(&current_path) {
            Ok(Some(opened))
                if self.tracked.as_ref().is_some_and(|tracked| opened.identity != tracked.opened.identity) =>
            {
                return Some(current_path);
            }
            Ok(Some(_)) => {}
            Ok(None) => self.mark_path_missing(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => self.mark_path_missing(),
            Err(err) => error!("Failed to open tracked path {}: {err}; retrying", current_path.display()),
        }

        let current_key = self.tracked.as_ref()?.opened.key;
        let discovered = self
            .stream_files(current_key.day)
            .into_iter()
            .filter_map(|path| StreamKey::from_path(&path).map(|key| (key, path)))
            .filter(|(key, _)| *key > current_key)
            .min_by_key(|(key, _)| *key);
        match (self.pending.as_ref(), discovered) {
            (Some(pending), Some((key, path))) if key < pending.key => Some(path),
            (Some(pending), _) => Some(pending.path.clone()),
            (None, Some((_, path))) => Some(path),
            (None, None) => None,
        }
    }

    pub(super) fn on_create(&mut self, path: &Path) -> FileRead {
        if let Some(candidate) = Self::open_candidate(path) {
            self.reconcile_current_identity();
            self.stage(candidate);
        }
        self.read_tracked()
    }

    pub(super) fn start_tracking(&mut self, path: &Path) {
        let Some(mut opened) = Self::open_candidate(path) else {
            return;
        };
        let Ok(metadata) = opened.file.metadata() else {
            return;
        };
        if opened.file.seek(SeekFrom::Start(metadata.len())).is_err() {
            return;
        }
        self.pending = None;
        self.tracked = Some(TrackedStream::new(opened, metadata.len()));
    }

    pub(super) fn read_tracked(&mut self) -> FileRead {
        let Some(tracked) = self.tracked.as_mut() else {
            if let Some(candidate) = self.pending.take() {
                self.tracked = Some(TrackedStream::new(candidate, 0));
                return FileRead::data();
            }
            return FileRead::idle();
        };

        let mut read = Self::read_stream(tracked, &mut self.read_buf);
        if matches!(read.progress, ReadProgress::Idle | ReadProgress::Retry)
            && let Some(candidate) = self.pending.take()
        {
            let preserve = read.progress != ReadProgress::Retry
                && candidate.key.is_immediate_successor_of(tracked.opened.key)
                && !tracked.path_missing
                && !tracked.read_unhealthy
                && tracked.partial_line.is_empty()
                && !tracked.discarding_oversize;
            if !preserve {
                read.continuity = FileContinuity::Lost;
            }
            self.tracked = Some(TrackedStream::new(candidate, 0));
            read.progress = ReadProgress::Data;
            read.unread_bytes = 0;
        }
        read
    }

    fn read_stream(tracked: &mut TrackedStream, read_buf: &mut [u8]) -> FileRead {
        let metadata = match tracked.opened.file.metadata() {
            Ok(metadata) => metadata,
            Err(err) => {
                error!("Failed to inspect open stream file {}: {err}", tracked.opened.path.display());
                return tracked.report_read_failure();
            }
        };

        let mut continuity = FileContinuity::Preserved;
        let state_changed = if metadata.len() < tracked.offset {
            if let Err(err) = tracked.opened.file.seek(SeekFrom::Start(0)) {
                error!("Failed to rewind stream file {}: {err}", tracked.opened.path.display());
                return tracked.report_read_failure();
            }
            tracked.offset = 0;
            tracked.partial_line.clear();
            tracked.discarding_oversize = false;
            continuity = FileContinuity::Lost;
            true
        } else {
            false
        };

        let bytes_read = loop {
            match tracked.opened.file.read(read_buf) {
                Ok(bytes) => break bytes,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => {
                    error!("Failed to read stream file {}: {err}", tracked.opened.path.display());
                    return tracked.report_read_failure();
                }
            }
        };
        tracked.mark_read_healthy();

        if bytes_read == 0 {
            return FileRead {
                continuity,
                unread_bytes: metadata.len().saturating_sub(tracked.offset),
                progress: if state_changed { ReadProgress::Data } else { ReadProgress::Idle },
                ..FileRead::idle()
            };
        }

        tracked.offset += bytes_read as u64;
        let (lines, framing_continuity) = tracked.frame(&read_buf[..bytes_read]);
        if framing_continuity == FileContinuity::Lost {
            continuity = FileContinuity::Lost;
        }

        FileRead {
            lines,
            continuity,
            bytes_read,
            unread_bytes: metadata.len().saturating_sub(tracked.offset),
            progress: ReadProgress::Data,
        }
    }

    fn stage(&mut self, candidate: OpenedStream) {
        let Some(current) = self.tracked.as_ref() else {
            self.pending = Some(candidate);
            return;
        };
        if candidate.identity == current.opened.identity || candidate.key < current.opened.key {
            return;
        }
        let replace = self.pending.as_ref().is_none_or(|pending| {
            candidate.key < pending.key || candidate.key == pending.key && candidate.path == current.opened.path
        });
        if replace {
            self.pending = Some(candidate);
        }
    }

    fn latest_initial_file(&self) -> Option<PathBuf> {
        let latest_day = std::fs::read_dir(self.base_dir.join("hourly"))
            .ok()?
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| {
                let day = NaiveDate::parse_from_str(entry.file_name().to_str()?, "%Y%m%d").ok()?;
                Some((day, entry.path()))
            })
            .max_by_key(|(day, _)| *day)?
            .1;
        std::fs::read_dir(latest_day)
            .ok()?
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
            .filter_map(|entry| {
                let path = entry.path();
                StreamKey::from_path(&path).map(|key| (key, path))
            })
            .max_by_key(|(key, _)| *key)
            .map(|(_, path)| path)
    }

    fn stream_files(&self, minimum_day: NaiveDate) -> Vec<PathBuf> {
        let Ok(days) = std::fs::read_dir(self.base_dir.join("hourly")) else {
            return Vec::new();
        };
        days.flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| NaiveDate::parse_from_str(name, "%Y%m%d").ok())
                    .is_some_and(|day| day >= minimum_day)
            })
            .flat_map(|entry| std::fs::read_dir(entry.path()).into_iter().flatten().flatten())
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
            .map(|entry| entry.path())
            .collect()
    }

    const fn mark_path_missing(&mut self) {
        if let Some(tracked) = self.tracked.as_mut() {
            tracked.path_missing = true;
        }
    }

    fn reconcile_current_identity(&mut self) {
        let Some((path, identity)) =
            self.tracked.as_ref().map(|tracked| (tracked.opened.path.clone(), tracked.opened.identity))
        else {
            return;
        };
        match OpenedStream::open(&path) {
            Ok(Some(opened)) if opened.identity == identity => {}
            Ok(Some(_) | None) => self.mark_path_missing(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => self.mark_path_missing(),
            Err(err) => {
                error!("Failed to verify tracked path {}: {err}", path.display());
                self.mark_path_missing();
            }
        }
    }

    fn open_candidate(path: &Path) -> Option<OpenedStream> {
        match OpenedStream::open(path) {
            Ok(candidate) => candidate,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                error!("Failed to open stream file {}: {err}; retrying", path.display());
                None
            }
        }
    }

    pub(super) fn current_path(&self) -> Option<&Path> {
        self.tracked.as_ref().map(|tracked| tracked.opened.path.as_path())
    }

    pub(super) const fn is_tracking(&self) -> bool {
        self.tracked.is_some()
    }

    #[cfg(test)]
    pub(super) fn file_position(&self) -> u64 {
        self.tracked.as_ref().map_or(0, |tracked| tracked.offset)
    }

    #[cfg(test)]
    pub(super) fn partial_line(&self) -> &str {
        self.tracked.as_ref().and_then(|tracked| std::str::from_utf8(&tracked.partial_line).ok()).unwrap_or("")
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn stream_test_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("orderbook-reader-{name}-{}", std::process::id()));
        drop(std::fs::remove_dir_all(&path));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn hour_file(base: &Path, hour: u8) -> PathBuf {
        let day = base.join("hourly/20260826");
        std::fs::create_dir_all(&day).unwrap();
        day.join(hour.to_string())
    }

    fn append(path: &Path, bytes: &[u8]) {
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(bytes).unwrap();
    }

    fn reconcile_and_read(reader: &mut FileReader) -> FileRead {
        match reader.check_for_newer_file() {
            Some(path) => reader.on_create(&path),
            None => reader.read_tracked(),
        }
    }

    #[test]
    fn reads_at_most_one_chunk_per_call() {
        let base = stream_test_dir("chunk-bound");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        append(&file, &vec![b'a'; READ_CHUNK_BYTES * 2 + 1]);

        assert_eq!(reader.read_tracked().bytes_read, READ_CHUNK_BYTES);
        assert_eq!(reader.partial_line().len(), READ_CHUNK_BYTES);
        assert_eq!(reader.read_tracked().bytes_read, READ_CHUNK_BYTES);
        assert_eq!(reader.read_tracked().bytes_read, 1);
        assert_eq!(reader.read_tracked().progress, ReadProgress::Idle);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn utf8_split_across_chunks_is_decoded_after_newline() {
        let base = stream_test_dir("utf8-split");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        let mut bytes = vec![b'a'; READ_CHUNK_BYTES - 1];
        bytes.extend_from_slice("🦀\n".as_bytes());
        append(&file, &bytes);

        assert!(reader.read_tracked().lines.is_empty());
        assert_eq!(reader.tracked.as_ref().unwrap().partial_line.len(), READ_CHUNK_BYTES);
        let second = reader.read_tracked();
        assert_eq!(second.lines, vec![format!("{}🦀", "a".repeat(READ_CHUNK_BYTES - 1))]);
        assert_eq!(second.continuity, FileContinuity::Preserved);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn invalid_utf8_loses_continuity_and_recovers_next_record() {
        let base = stream_test_dir("invalid-utf8");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        append(&file, b"\xff\n{\"ok\":true}\n");

        let read = reader.read_tracked();
        assert_eq!(read.continuity, FileContinuity::Lost);
        assert_eq!(read.lines, vec![r#"{"ok":true}"#]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn oversized_record_is_bounded_and_recovers_at_newline() {
        let base = stream_test_dir("oversize");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        append(&file, &vec![b'x'; MAX_PARTIAL_LINE_BYTES + 1]);
        append(&file, b"\n{}\n");

        let mut continuity_lost = false;
        let mut lines = Vec::new();
        loop {
            let read = reader.read_tracked();
            continuity_lost |= read.continuity == FileContinuity::Lost;
            lines.extend(read.lines);
            assert!(reader.tracked.as_ref().unwrap().partial_line.len() <= MAX_PARTIAL_LINE_BYTES);
            assert!(reader.tracked.as_ref().unwrap().partial_line.capacity() <= MAX_PARTIAL_LINE_BYTES);
            if read.progress == ReadProgress::Idle {
                break;
            }
        }
        assert!(continuity_lost);
        assert_eq!(lines, vec!["{}"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn exact_limit_record_and_remainder_fit_batch_budget() {
        let base = stream_test_dir("exact-limit");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        append(&file, &vec![b'x'; MAX_PARTIAL_LINE_BYTES]);
        append(&file, b"\n{}\n");

        let read = loop {
            let read = reader.read_tracked();
            if !read.lines.is_empty() {
                break read;
            }
        };
        assert_eq!(read.lines.len(), 2);
        assert_eq!(read.lines[0].len(), MAX_PARTIAL_LINE_BYTES);
        assert_eq!(read.lines[1], "{}");
        let charged =
            read.lines.capacity() * size_of::<String>() + read.lines.iter().map(String::capacity).sum::<usize>();
        assert!(charged <= 32 * 1024 * 1024, "batch charged {charged} bytes");
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn truncation_clears_partial_and_reads_from_zero() {
        let base = stream_test_dir("truncate");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        append(&file, b"partial");
        assert_eq!(reader.read_tracked().progress, ReadProgress::Data);
        std::fs::write(&file, b"{}\n").unwrap();

        let read = reader.read_tracked();
        assert_eq!(read.continuity, FileContinuity::Lost);
        assert_eq!(read.lines, vec!["{}"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn large_predecessor_drains_in_chunks_before_successor() {
        let base = stream_test_dir("large-rotation");
        let old = hour_file(&base, 1);
        let new = hour_file(&base, 2);
        std::fs::write(&old, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&old);
        let old_line = format!("{}\n", "x".repeat(READ_CHUNK_BYTES * 2));
        append(&old, old_line.as_bytes());
        std::fs::write(&new, b"new\n").unwrap();

        let first = reader.on_create(&new);
        assert_eq!(first.bytes_read, READ_CHUNK_BYTES);
        assert_eq!(reader.current_path(), Some(old.as_path()));
        assert_eq!(reader.read_tracked().bytes_read, READ_CHUNK_BYTES);
        assert_eq!(reader.current_path(), Some(old.as_path()));
        assert_eq!(reader.read_tracked().bytes_read, 1);
        let switched = reader.read_tracked();
        assert_eq!(switched.progress, ReadProgress::Data);
        assert_eq!(reader.current_path(), Some(new.as_path()));
        assert_eq!(reader.read_tracked().lines, vec!["new"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn earliest_successor_is_preferred() {
        let base = stream_test_dir("earliest");
        let current = hour_file(&base, 1);
        std::fs::write(&current, []).unwrap();
        std::fs::write(hour_file(&base, 3), []).unwrap();
        std::fs::write(hour_file(&base, 2), []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&current);

        assert_eq!(reader.check_for_newer_file(), Some(hour_file(&base, 2)));
        std::fs::remove_dir_all(base).unwrap();
    }

    fn finish_switch(reader: &mut FileReader, successor: &Path) -> FileRead {
        while reader.current_path() != Some(successor) {
            reader.read_tracked();
        }
        reader.read_tracked()
    }

    #[test]
    fn repeated_read_failure_reports_one_loss_until_healthy() {
        let base = stream_test_dir("failure-latch");
        let file = hour_file(&base, 1);
        std::fs::write(&file, []).unwrap();
        let opened = FileReader::open_candidate(&file).unwrap();
        let mut tracked = TrackedStream::new(opened, 0);
        assert_eq!(tracked.report_read_failure().continuity, FileContinuity::Lost);
        assert_eq!(tracked.report_read_failure().continuity, FileContinuity::Preserved);
        tracked.mark_read_healthy();
        assert_eq!(tracked.report_read_failure().continuity, FileContinuity::Lost);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn invalid_start_and_directory_create_are_ignored() {
        let base = stream_test_dir("invalid-candidates");
        let file = hour_file(&base, 1);
        std::fs::write(&file, b"old\n").unwrap();
        let day = file.parent().unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        let position = reader.file_position();

        reader.start_tracking(day);
        reader.start_tracking(&day.join("missing"));
        reader.on_create(day);
        assert_eq!(reader.current_path(), Some(file.as_path()));
        assert_eq!(reader.file_position(), position);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn duplicate_create_preserves_offset_without_replay() {
        let base = stream_test_dir("duplicate-create");
        let file = hour_file(&base, 1);
        std::fs::write(&file, b"old\n").unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        let position = reader.file_position();
        assert_eq!(reader.on_create(&file).progress, ReadProgress::Idle);
        assert_eq!(reader.file_position(), position);
        append(&file, b"new\n");
        assert_eq!(reconcile_and_read(&mut reader).lines, vec!["new"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn first_modify_attaches_at_eof() {
        let base = stream_test_dir("first-modify");
        let file = hour_file(&base, 1);
        std::fs::write(&file, b"old\n").unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        assert_eq!(reader.read_tracked().progress, ReadProgress::Idle);
        append(&file, b"new\n");
        assert_eq!(reader.read_tracked().lines, vec!["new"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn same_inode_shrink_rewinds_and_reports_loss() {
        let base = stream_test_dir("shrink");
        let file = hour_file(&base, 1);
        std::fs::write(&file, b"long-existing-line\n").unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&file);
        std::fs::OpenOptions::new().write(true).truncate(true).open(&file).unwrap().write_all(b"{}\n").unwrap();
        let read = reader.read_tracked();
        assert_eq!(read.continuity, FileContinuity::Lost);
        assert_eq!(read.lines, vec!["{}"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn same_path_replacement_drains_old_and_restarts_at_zero() {
        for (name, replacement) in [("equal", b"new\n".as_slice()), ("larger", b"replacement\n".as_slice())] {
            let base = stream_test_dir(name);
            let file = hour_file(&base, 1);
            std::fs::write(&file, b"old\n").unwrap();
            let mut reader = FileReader::new(base.clone());
            reader.start_tracking(&file);
            let temporary = file.with_extension("replacement");
            std::fs::write(&temporary, replacement).unwrap();
            std::fs::rename(&temporary, &file).unwrap();

            let staged = reconcile_and_read(&mut reader);
            assert_eq!(staged.continuity, FileContinuity::Lost);
            let read = finish_switch(&mut reader, &file);
            assert_eq!(read.lines, vec![std::str::from_utf8(replacement).unwrap().trim_end()]);
            std::fs::remove_dir_all(base).unwrap();
        }
    }

    #[test]
    fn missing_predecessor_is_drained_then_reports_loss() {
        let base = stream_test_dir("missing-predecessor");
        let old = hour_file(&base, 1);
        let new = hour_file(&base, 2);
        std::fs::write(&old, []).unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&old);
        append(&old, b"old\n");
        std::fs::remove_file(&old).unwrap();
        std::fs::write(&new, b"new\n").unwrap();
        assert_eq!(reader.check_for_newer_file(), Some(new.clone()));
        let first = reader.on_create(&new);
        assert_eq!(first.lines, vec!["old"]);
        let transition = reader.read_tracked();
        assert_eq!(transition.continuity, FileContinuity::Lost);
        assert_eq!(finish_switch(&mut reader, &new).lines, vec!["new"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn unreadable_predecessor_switches_to_healthy_successor() {
        let base = stream_test_dir("unreadable-predecessor");
        let old = hour_file(&base, 1);
        let new = hour_file(&base, 2);
        std::fs::write(&old, []).unwrap();
        std::fs::write(&new, b"new\n").unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&old);
        reader.tracked.as_mut().unwrap().opened.file = std::fs::OpenOptions::new().write(true).open(&old).unwrap();

        let switched = reader.on_create(&new);
        assert_eq!(switched.continuity, FileContinuity::Lost);
        assert_eq!(switched.progress, ReadProgress::Data);
        assert_eq!(reader.current_path(), Some(new.as_path()));
        assert_eq!(reader.read_tracked().lines, vec!["new"]);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn skipped_hour_and_trailing_partial_report_loss() {
        for (name, old_hour, new_hour, tail) in
            [("skipped", 1, 3, b"".as_slice()), ("partial", 1, 2, b"partial".as_slice())]
        {
            let base = stream_test_dir(name);
            let old = hour_file(&base, old_hour);
            let new = hour_file(&base, new_hour);
            std::fs::write(&old, []).unwrap();
            let mut reader = FileReader::new(base.clone());
            reader.start_tracking(&old);
            append(&old, tail);
            std::fs::write(&new, b"new\n").unwrap();
            let mut lost = reader.on_create(&new).continuity == FileContinuity::Lost;
            while reader.current_path() != Some(new.as_path()) {
                lost |= reader.read_tracked().continuity == FileContinuity::Lost;
            }
            assert!(lost);
            assert_eq!(reader.read_tracked().lines, vec!["new"]);
            std::fs::remove_dir_all(base).unwrap();
        }
    }

    #[test]
    fn day_rollover_and_ordinary_rotation_preserve_continuity() {
        for rollover in [false, true] {
            let base = stream_test_dir(if rollover { "rollover" } else { "ordinary" });
            let old = if rollover {
                let day = base.join("hourly/20260826");
                std::fs::create_dir_all(&day).unwrap();
                day.join("23")
            } else {
                hour_file(&base, 1)
            };
            let new = if rollover {
                let day = base.join("hourly/20260827");
                std::fs::create_dir_all(&day).unwrap();
                day.join("0")
            } else {
                hour_file(&base, 2)
            };
            std::fs::write(&old, []).unwrap();
            let mut reader = FileReader::new(base.clone());
            reader.start_tracking(&old);
            append(&old, b"old\n");
            std::fs::write(&new, b"new\n").unwrap();
            let old_read = reader.on_create(&new);
            assert_eq!(old_read.lines, vec!["old"]);
            assert_eq!(old_read.continuity, FileContinuity::Preserved);
            let transition = reader.read_tracked();
            assert_eq!(transition.continuity, FileContinuity::Preserved);
            assert_eq!(reader.read_tracked().lines, vec!["new"]);
            std::fs::remove_dir_all(base).unwrap();
        }
    }

    #[test]
    fn delayed_older_create_is_ignored() {
        let base = stream_test_dir("older-create");
        let older = hour_file(&base, 1);
        let current = hour_file(&base, 2);
        std::fs::write(&older, b"older\n").unwrap();
        std::fs::write(&current, b"current\n").unwrap();
        let mut reader = FileReader::new(base.clone());
        reader.start_tracking(&current);
        assert_eq!(reader.on_create(&older).progress, ReadProgress::Idle);
        assert_eq!(reader.current_path(), Some(current.as_path()));
        std::fs::remove_dir_all(base).unwrap();
    }
}
