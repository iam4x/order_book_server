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

    pub(super) fn attach(base_dir: PathBuf) -> std::io::Result<Self> {
        let mut reader = Self::new(base_dir);
        if let Some(path) = reader.latest_initial_file()? {
            let mut opened = OpenedStream::open(&path)?.ok_or_else(|| {
                std::io::Error::other(format!("Stream disappeared during startup: {}", path.display()))
            })?;
            let end = opened.file.metadata()?.len();
            let mut offset = end;
            let floor = end.saturating_sub(MAX_PARTIAL_LINE_BYTES as u64 + 1);
            while offset > floor {
                let start = offset.saturating_sub(READ_CHUNK_BYTES as u64).max(floor);
                let length = usize::try_from(offset - start).unwrap_or(READ_CHUNK_BYTES);
                opened.file.seek(SeekFrom::Start(start))?;
                opened.file.read_exact(&mut reader.read_buf[..length])?;
                if let Some(newline) = reader.read_buf[..length].iter().rposition(|&byte| byte == b'\n') {
                    offset = start + newline as u64 + 1;
                    break;
                }
                offset = start;
            }
            if end - offset > MAX_PARTIAL_LINE_BYTES as u64 {
                return Err(std::io::Error::other("Initial stream tail exceeds the maximum record size"));
            }
            opened.file.seek(SeekFrom::Start(offset))?;
            reader.tracked = Some(TrackedStream::new(opened, offset));
        }
        Ok(reader)
    }

    pub(super) fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        let Some(current_path) = self.tracked.as_ref().map(|tracked| tracked.opened.path.clone()) else {
            return self
                .stream_files(NaiveDate::MIN)
                .into_iter()
                .filter_map(|path| StreamKey::from_path(&path).map(|key| (key, path)))
                .min_by_key(|(key, _)| *key)
                .map(|(_, path)| path);
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

    #[cfg(test)]
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

    fn latest_initial_file(&self) -> std::io::Result<Option<PathBuf>> {
        let days = match std::fs::read_dir(self.base_dir.join("hourly")) {
            Ok(days) => days,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut candidates = Vec::new();
        for entry in days {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(day) =
                entry.file_name().to_str().and_then(|name| NaiveDate::parse_from_str(name, "%Y%m%d").ok())
            {
                candidates.push((day, entry.path()));
            }
        }
        candidates.sort_unstable_by_key(|(day, _)| std::cmp::Reverse(*day));
        for (_, day) in candidates {
            let mut latest = None;
            for entry in std::fs::read_dir(day)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let path = entry.path();
                if let Some(key) = StreamKey::from_path(&path)
                    && latest.as_ref().is_none_or(|(last, _)| key > *last)
                {
                    latest = Some((key, path));
                }
            }
            if let Some((_, path)) = latest {
                return Ok(Some(path));
            }
        }
        Ok(None)
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
mod tests;
