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
    let charged = read.lines.capacity() * size_of::<String>() + read.lines.iter().map(String::capacity).sum::<usize>();
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

#[test]
fn startup_preserves_an_incomplete_record_and_ignores_complete_history() {
    let base = stream_test_dir("attach-partial");
    let file = hour_file(&base, 1);
    std::fs::write(&file, b"history\n{\"block_number\":").unwrap();
    let mut reader = FileReader::attach(base.clone()).unwrap();
    append(&file, b"42}\n");
    assert_eq!(reader.read_tracked().lines, ["{\"block_number\":42}"]);
    assert_eq!(reader.read_tracked().progress, ReadProgress::Idle);
    std::fs::remove_dir_all(base).unwrap();
}

#[test]
fn startup_accepts_a_maximum_size_partial_record() {
    let base = stream_test_dir("attach-max-partial");
    let file = hour_file(&base, 1);
    std::fs::write(&file, b"history\n").unwrap();
    append(&file, &vec![b'x'; MAX_PARTIAL_LINE_BYTES]);
    let mut reader = FileReader::attach(base.clone()).unwrap();
    assert_eq!(reader.file_position(), 8);
    append(&file, b"\n");
    let mut lines = Vec::new();
    loop {
        let read = reader.read_tracked();
        assert_eq!(read.continuity, FileContinuity::Preserved);
        lines.extend(read.lines);
        if read.progress == ReadProgress::Idle {
            break;
        }
    }
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].len(), MAX_PARTIAL_LINE_BYTES);
    std::fs::remove_dir_all(base).unwrap();
}

#[test]
fn startup_finds_the_latest_file_before_an_empty_day_directory() {
    let base = stream_test_dir("attach-empty-day");
    let file = hour_file(&base, 1);
    std::fs::write(&file, b"history\n").unwrap();
    std::fs::create_dir_all(base.join("hourly/20260827")).unwrap();
    let reader = FileReader::attach(base.clone()).unwrap();
    assert_eq!(reader.current_path(), Some(file.as_path()));
    assert_eq!(reader.file_position(), 8);
    std::fs::remove_dir_all(base).unwrap();
}
