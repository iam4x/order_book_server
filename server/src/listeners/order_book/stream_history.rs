use std::{
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use log::warn;
use sonic_rs::JsonValueTrait;

use super::{SnapshotReplayOutcome, parallel::StreamKey, state::OrderBookState};
use crate::{
    prelude::*,
    types::node_data::{Batch, EventSource, NodeDataOrderDiff, NodeDataOrderStatus},
};

/// Replay the stream lines hl-node keeps on disk for blocks in
/// `(after_height, through_height]`. Sources are merged by block number so the
/// two halves of a status-paired order meet before the pending caches overflow.
pub(super) fn replay_stream_history(
    state: &mut OrderBookState,
    data_dir: &Path,
    sources: &[EventSource],
    after_height: u64,
    through_height: u64,
    replay_guard_height: u64,
) -> Result<SnapshotReplayOutcome> {
    let mut outcome = SnapshotReplayOutcome { replayed_lines: 0, clean: true };
    let mut cursors = sources
        .iter()
        .map(|&source| SourceCursor::open(source, data_dir, after_height, through_height, &mut outcome.clean))
        .collect::<Result<Vec<_>>>()?;
    let count = cursors.len();
    let mut last = count.saturating_sub(1);
    // Within a block, rotate between sources line by line like the live receiver,
    // so a large block cannot overflow one side's pending cache before the other arrives.
    while let Some((_, _, index)) = cursors
        .iter()
        .enumerate()
        .filter_map(|(index, cursor)| {
            cursor.head.as_ref().map(|(block, _)| (*block, (index + count - last - 1) % count, index))
        })
        .min()
    {
        last = index;
        let cursor = &mut cursors[index];
        if let Some((_, line)) = cursor.head.take() {
            apply_history_line(state, cursor.source, &line, replay_guard_height, &mut outcome);
        }
        cursor.advance(&mut outcome.clean)?;
    }
    Ok(outcome)
}

fn apply_history_line(
    state: &mut OrderBookState,
    source: EventSource,
    line: &[u8],
    replay_guard_height: u64,
    outcome: &mut SnapshotReplayOutcome,
) {
    let applied = match source {
        EventSource::OrderDiffs => sonic_rs::from_slice::<Batch<NodeDataOrderDiff>>(line)
            .map(|batch| state.apply_order_diffs_after_snapshot(batch, replay_guard_height)),
        EventSource::OrderStatuses => {
            sonic_rs::from_slice::<Batch<NodeDataOrderStatus>>(line).map(|batch| state.apply_order_statuses_hft(batch))
        }
        EventSource::Fills => return,
    };
    match applied {
        Ok(Ok(_)) => outcome.replayed_lines += 1,
        Ok(Err(err)) => {
            warn!("Skipping {source} history line that failed to apply: {err}");
            outcome.replayed_lines += 1;
            outcome.clean = false;
        }
        Err(err) => {
            warn!("Skipping unparsable {source} history line: {err}");
            outcome.clean = false;
        }
    }
}

struct SourceCursor {
    source: EventSource,
    reader: BufReader<File>,
    next_files: std::vec::IntoIter<PathBuf>,
    after_height: u64,
    through_height: u64,
    head: Option<(u64, Vec<u8>)>,
}

impl SourceCursor {
    fn open(
        source: EventSource,
        data_dir: &Path,
        after_height: u64,
        through_height: u64,
        clean: &mut bool,
    ) -> Result<Self> {
        let dir = source.event_source_dir_streaming(data_dir);
        let files = hourly_files(&dir)?;
        let mut start = None;
        for (index, path) in files.iter().enumerate() {
            if first_block(path)?.is_some_and(|block| block <= after_height) {
                start = Some(index);
            }
        }
        let Some(start) = start else {
            return Err(
                format!("{source} history in {} does not reach back to block {after_height}", dir.display()).into()
            );
        };
        let mut files = files.into_iter().skip(start);
        let Some(path) = files.next() else {
            return Err(format!("{source} history start file disappeared").into());
        };
        let mut reader = BufReader::new(File::open(&path)?);
        let len = reader.get_ref().metadata()?.len();
        let offset = first_offset_after(&mut reader, len, after_height)?;
        reader.seek(SeekFrom::Start(offset))?;
        let mut cursor = Self {
            source,
            reader,
            next_files: files.collect::<Vec<_>>().into_iter(),
            after_height,
            through_height,
            head: None,
        };
        cursor.advance(clean)?;
        Ok(cursor)
    }

    /// Load the next line inside the replay window into `head`, or leave it empty
    /// once the source reaches a block past `through_height`.
    fn advance(&mut self, clean: &mut bool) -> Result<()> {
        loop {
            let mut line = Vec::new();
            let read = self.reader.read_until(b'\n', &mut line)?;
            if read == 0 || !line.ends_with(b"\n") {
                let Some(path) = self.next_files.next() else {
                    return Err(format!(
                        "{} history on disk ends before block {}",
                        self.source,
                        self.through_height.saturating_add(1)
                    )
                    .into());
                };
                self.reader = BufReader::new(File::open(path)?);
                continue;
            }
            let Some(block) = block_number(&line) else {
                warn!("Skipping {} history line without a block number", self.source);
                *clean = false;
                continue;
            };
            if block > self.through_height {
                self.head = None;
                return Ok(());
            }
            if block > self.after_height {
                self.head = Some((block, line));
                return Ok(());
            }
        }
    }
}

fn hourly_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let hourly_dir = dir.join("hourly");
    let mut files = Vec::new();
    for day in fs::read_dir(&hourly_dir).map_err(|err| format!("Failed to list {}: {err}", hourly_dir.display()))? {
        let day = day?;
        if !day.file_type()?.is_dir() {
            continue;
        }
        for entry in fs::read_dir(day.path())? {
            let path = entry?.path();
            if let Some(key) = StreamKey::from_path(&path) {
                files.push((key, path));
            }
        }
    }
    files.sort_unstable_by_key(|(key, _)| *key);
    Ok(files.into_iter().map(|(_, path)| path).collect())
}

fn first_block(path: &Path) -> Result<Option<u64>> {
    let mut line = Vec::new();
    BufReader::new(File::open(path)?).read_until(b'\n', &mut line)?;
    Ok(if line.ends_with(b"\n") { block_number(&line) } else { None })
}

/// Binary-search for a line start such that every earlier line has a block at or
/// below `after_height`. Lines past it are filtered again while reading, because
/// hl-node restarts can rewrite older blocks later in a file.
fn first_offset_after(reader: &mut BufReader<File>, len: u64, after_height: u64) -> Result<u64> {
    let mut lo = 0;
    let mut hi = len;
    let mut line = Vec::new();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        // Seeking one byte back lands the skip on `mid` itself when it already starts a line.
        let skip_from = mid.saturating_sub(1);
        reader.seek(SeekFrom::Start(skip_from))?;
        line.clear();
        let start = if mid == 0 { 0 } else { skip_from + reader.read_until(b'\n', &mut line)? as u64 };
        line.clear();
        let read = reader.read_until(b'\n', &mut line)? as u64;
        let block = if line.ends_with(b"\n") { block_number(&line) } else { None };
        match block {
            Some(block) if block <= after_height => lo = start + read,
            _ => hi = mid,
        }
    }
    Ok(lo)
}

fn block_number(line: &[u8]) -> Option<u64> {
    sonic_rs::get(line, ["block_number"]).ok()?.as_u64()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{BookMode, order_book::multi_book::Snapshots};

    fn history_test_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("orderbook-history-{name}-{}", std::process::id()));
        drop(fs::remove_dir_all(&path));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write_hour(data_dir: &Path, source: EventSource, day: &str, hour: u8, lines: &[String], tail: &str) {
        let day_dir = source.event_source_dir_streaming(data_dir).join("hourly").join(day);
        fs::create_dir_all(&day_dir).unwrap();
        let mut contents = lines.join("\n");
        if !lines.is_empty() {
            contents.push('\n');
        }
        contents.push_str(tail);
        fs::write(day_dir.join(hour.to_string()), contents).unwrap();
    }

    fn new_bid_line(oid: u64, block: u64) -> String {
        serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": block,
            "events": [{
                "user": "0x0000000000000000000000000000000000000000",
                "oid": oid,
                "side": "B",
                "px": "100",
                "coin": "BTC",
                "raw_book_diff": { "new": { "sz": "1" } }
            }]
        })
        .to_string()
    }

    fn open_status_line(oid: u64, block: u64) -> String {
        let user = "0x0000000000000000000000000000000000000001";
        serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": block,
            "events": [{
                "time": "2024-01-15T10:30:00",
                "user": user,
                "hash": "0xabc",
                "builder": null,
                "status": "open",
                "order": {
                    "user": user,
                    "coin": "BTC",
                    "side": "B",
                    "limitPx": "100.0",
                    "sz": "1.0",
                    "oid": oid,
                    "timestamp": 1000,
                    "triggerCondition": "N/A",
                    "isTrigger": false,
                    "triggerPx": "0.0",
                    "children": [],
                    "isPositionTpsl": false,
                    "reduceOnly": false,
                    "orderType": "Limit",
                    "origSz": "1.0",
                    "tif": "Gtc",
                    "cloid": null
                }
            }]
        })
        .to_string()
    }

    fn bid_lines(blocks: std::ops::RangeInclusive<u64>) -> Vec<String> {
        blocks.map(|block| new_bid_line(block, block)).collect()
    }

    fn empty_state(height: u64, book_mode: BookMode) -> OrderBookState {
        OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), height, 0, false, book_mode)
    }

    fn btc_bid_oids(state: &OrderBookState) -> Vec<u64> {
        let books = state.compute_snapshot().snapshot.value();
        let mut oids: Vec<u64> = books
            .get(&crate::order_book::Coin::new("BTC"))
            .map_or_else(Vec::new, |book| book.as_ref()[0].iter().map(|order| order.oid).collect());
        oids.sort_unstable();
        oids
    }

    fn two_hour_history(name: &str) -> PathBuf {
        let data_dir = history_test_dir(name);
        write_hour(&data_dir, EventSource::OrderDiffs, "20261019", 23, &bid_lines(1..=40), "");
        write_hour(&data_dir, EventSource::OrderDiffs, "20261020", 0, &bid_lines(41..=80), "{\"block_number\":81");
        data_dir
    }

    #[test]
    fn history_replay_starts_mid_file_and_continues_into_the_next_hour() {
        let data_dir = two_hour_history("across-hours");
        let mut state = empty_state(30, BookMode::DiffsOnly);

        let outcome = replay_stream_history(&mut state, &data_dir, &[EventSource::OrderDiffs], 30, 45, 30).unwrap();

        assert_eq!(outcome.replayed_lines, 15);
        assert!(outcome.clean);
        assert_eq!(btc_bid_oids(&state), (31..=45).collect::<Vec<_>>());
    }

    #[test]
    fn history_replay_starts_in_the_middle_of_the_latest_eligible_file() {
        let data_dir = two_hour_history("mid-file");
        let mut state = empty_state(60, BookMode::DiffsOnly);

        let outcome = replay_stream_history(&mut state, &data_dir, &[EventSource::OrderDiffs], 60, 70, 60).unwrap();

        assert_eq!(outcome.replayed_lines, 10);
        assert_eq!(btc_bid_oids(&state), (61..=70).collect::<Vec<_>>());
    }

    #[test]
    fn history_replay_keeps_the_part_of_the_first_block_written_before_an_hour_rollover() {
        let data_dir = history_test_dir("split-block");
        let mut older = bid_lines(1..=10);
        older.push(new_bid_line(111, 11));
        write_hour(&data_dir, EventSource::OrderDiffs, "20261019", 23, &older, "");
        let mut newer = vec![new_bid_line(112, 11)];
        newer.extend(bid_lines(12..=20));
        write_hour(&data_dir, EventSource::OrderDiffs, "20261020", 0, &newer, "");
        let mut state = empty_state(10, BookMode::DiffsOnly);

        let outcome = replay_stream_history(&mut state, &data_dir, &[EventSource::OrderDiffs], 10, 12, 10).unwrap();

        assert_eq!(outcome.replayed_lines, 3);
        assert!(outcome.clean);
        assert_eq!(btc_bid_oids(&state), vec![12, 111, 112]);
    }

    #[test]
    fn history_replay_fails_when_disk_history_starts_after_the_state_height() {
        let data_dir = history_test_dir("starts-late");
        write_hour(&data_dir, EventSource::OrderDiffs, "20261020", 0, &bid_lines(41..=80), "");
        let mut state = empty_state(30, BookMode::DiffsOnly);

        let err = replay_stream_history(&mut state, &data_dir, &[EventSource::OrderDiffs], 30, 45, 30)
            .err()
            .unwrap()
            .to_string();

        assert!(err.contains("does not reach back to block 30"), "{err}");
        assert_eq!(btc_bid_oids(&state), Vec::<u64>::new());
    }

    #[test]
    fn history_replay_fails_when_disk_ends_before_the_block_after_the_cutoff() {
        let data_dir = two_hour_history("ends-early");
        let mut state = empty_state(60, BookMode::DiffsOnly);

        let err = replay_stream_history(&mut state, &data_dir, &[EventSource::OrderDiffs], 60, 80, 60)
            .err()
            .unwrap()
            .to_string();

        assert_eq!(err, "OrderDiffs history on disk ends before block 81");
    }

    #[test]
    fn history_replay_merges_status_paired_sources_by_block() {
        let data_dir = history_test_dir("status-paired");
        let blocks = 0..=10_002;
        write_hour(&data_dir, EventSource::OrderDiffs, "20261020", 0, &bid_lines(blocks.clone()), "");
        let statuses: Vec<String> = blocks.map(|block| open_status_line(block, block)).collect();
        write_hour(&data_dir, EventSource::OrderStatuses, "20261020", 0, &statuses, "");
        let mut state = empty_state(0, BookMode::StatusPaired);

        let outcome = replay_stream_history(
            &mut state,
            &data_dir,
            &[EventSource::OrderDiffs, EventSource::OrderStatuses],
            0,
            10_001,
            0,
        )
        .unwrap();

        assert_eq!(outcome.replayed_lines, 20_002);
        assert!(outcome.clean);
        assert_eq!(state.order_count(), 10_001);
        assert_eq!(state.pending_new_diffs_count(), 0);
        assert!(state.take_repair_reasons().is_empty());
    }

    #[test]
    fn history_replay_interleaves_sources_within_a_block() {
        let data_dir = history_test_dir("same-block");
        let oids = 1..=10_001;
        let mut diffs = vec![new_bid_line(0, 0)];
        diffs.extend(oids.clone().map(|oid| new_bid_line(oid, 1)));
        diffs.push(new_bid_line(0, 2));
        write_hour(&data_dir, EventSource::OrderDiffs, "20261020", 0, &diffs, "");
        let mut statuses = vec![open_status_line(0, 0)];
        statuses.extend(oids.map(|oid| open_status_line(oid, 1)));
        statuses.push(open_status_line(0, 2));
        write_hour(&data_dir, EventSource::OrderStatuses, "20261020", 0, &statuses, "");
        let mut state = empty_state(0, BookMode::StatusPaired);

        let outcome = replay_stream_history(
            &mut state,
            &data_dir,
            &[EventSource::OrderDiffs, EventSource::OrderStatuses],
            0,
            1,
            0,
        )
        .unwrap();

        assert_eq!(outcome.replayed_lines, 20_002);
        assert!(outcome.clean);
        assert_eq!(state.order_count(), 10_001);
        assert_eq!(state.pending_new_diffs_count(), 0);
        assert!(state.take_repair_reasons().is_empty());
    }
}
