use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use super::*;

const INPUT_BYTES: u64 = 64 * 1024 * 1024;
const CONCURRENT_INPUT_BYTES: u64 = 16 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(120);

struct BenchmarkStream {
    base_dir: PathBuf,
    path: PathBuf,
}

impl BenchmarkStream {
    fn new() -> Self {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock should be valid").as_nanos();
        let base_dir = std::env::temp_dir().join(format!("orderbook-reader-benchmark-{}-{nonce}", std::process::id()));
        let day_dir = base_dir.join("hourly/20260916");
        std::fs::create_dir_all(&day_dir).expect("benchmark stream directory should be created");
        let path = day_dir.join("19");
        File::create(&path).expect("benchmark stream file should be created");
        Self { base_dir, path }
    }
}

impl Drop for BenchmarkStream {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.base_dir));
    }
}

fn benchmark_line(index: u64) -> String {
    format!(
        r#"{{"block_number":{index},"sequence":{index},"payload":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}}"#
    )
}

fn append_input(path: &Path, target_bytes: u64) -> (u64, u64) {
    let file = OpenOptions::new().append(true).open(path).expect("benchmark stream file should open");
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut bytes = 0u64;
    let mut lines = 0u64;
    while bytes < target_bytes {
        let line = benchmark_line(lines);
        writer.write_all(line.as_bytes()).expect("benchmark line should append");
        writer.write_all(b"\n").expect("benchmark newline should append");
        bytes += line.len() as u64 + 1;
        lines += 1;
    }
    writer.flush().expect("benchmark input should flush");
    (bytes, lines)
}

fn input_line_count(target_bytes: u64) -> u64 {
    let mut bytes = 0u64;
    let mut lines = 0u64;
    while bytes < target_bytes {
        bytes += benchmark_line(lines).len() as u64 + 1;
        lines += 1;
    }
    lines
}

struct ProducerStats {
    raw_bytes: u64,
    returned_bytes: u64,
    lines: u64,
    largest_read_bytes: u64,
    read_calls: u64,
    batches_submitted: u64,
}

fn read_and_submit(reader: &mut FileReader, sink: &FileLineSink) -> (ReadProgress, ProducerStats) {
    let read = reader.read_tracked();
    assert_eq!(read.continuity, FileContinuity::Preserved);
    assert_ne!(read.progress, ReadProgress::Retry);
    let returned_bytes = read.lines.iter().map(|line| line.len() as u64 + 1).sum::<u64>();
    let lines = read.lines.len() as u64;
    let batches_submitted = u64::from(!read.lines.is_empty());
    let progress = read.progress;
    let raw_bytes = read.bytes_read as u64;
    assert!(submit_file_read(sink, read));
    (
        progress,
        ProducerStats {
            raw_bytes,
            returned_bytes,
            lines,
            largest_read_bytes: returned_bytes,
            read_calls: 1,
            batches_submitted,
        },
    )
}

fn add_stats(total: &mut ProducerStats, read: ProducerStats) {
    total.raw_bytes += read.raw_bytes;
    total.returned_bytes += read.returned_bytes;
    total.lines += read.lines;
    total.largest_read_bytes = total.largest_read_bytes.max(read.largest_read_bytes);
    total.read_calls += read.read_calls;
    total.batches_submitted += read.batches_submitted;
}

async fn drain_order_diffs(receiver: &mut FileEventReceiver) -> u64 {
    let mut index = 0u64;
    let mut batch = Vec::with_capacity(4096);
    loop {
        batch.clear();
        let count = receiver.recv_many(&mut batch, 4096).await;
        if count == 0 {
            break;
        }
        for event in &batch {
            let FileEvent::OrderDiff(line) = event else {
                panic!("benchmark received an unexpected event: {event:?}");
            };
            assert_eq!(line, &benchmark_line(index));
            index += 1;
        }
    }
    index
}

fn vm_hwm_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:")?.split_whitespace().next()?.parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "release throughput benchmark"]
async fn release_file_reader_channel_throughput() {
    let stream = BenchmarkStream::new();
    let mut reader = FileReader::new(stream.base_dir.clone());
    reader.start_tracking(&stream.path);
    let (input_bytes, expected_lines) = append_input(&stream.path, INPUT_BYTES);
    let (senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs], 16_384);
    let sink = FileLineSink::Events { source: EventSource::OrderDiffs, tx: senders.into_iter().next().unwrap() };
    let started = Instant::now();

    let producer = tokio::task::spawn_blocking(move || {
        let mut total = ProducerStats {
            raw_bytes: 0,
            returned_bytes: 0,
            lines: 0,
            largest_read_bytes: 0,
            read_calls: 0,
            batches_submitted: 0,
        };
        loop {
            let (progress, read) = read_and_submit(&mut reader, &sink);
            add_stats(&mut total, read);
            if progress == ReadProgress::Idle {
                break;
            }
        }
        drop(sink);
        total
    });

    let drained_lines = tokio::time::timeout(TIMEOUT, drain_order_diffs(&mut receiver))
        .await
        .expect("benchmark pipeline exceeded its time bound");

    let producer = producer.await.expect("benchmark producer should finish");
    let elapsed = started.elapsed();
    assert_eq!(producer.lines, expected_lines);
    assert_eq!(producer.raw_bytes, input_bytes);
    assert_eq!(producer.returned_bytes, input_bytes);
    assert_eq!(drained_lines, expected_lines);
    assert_eq!(std::fs::metadata(&stream.path).unwrap().len(), input_bytes);

    let mib_per_second = input_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    println!(
        "bytes={input_bytes} lines={drained_lines} elapsed={elapsed:?} throughput_mib_s={mib_per_second:.2} read_calls={} batches_submitted={} largest_read_return_bytes={} vm_hwm_kib={}",
        producer.read_calls,
        producer.batches_submitted,
        producer.largest_read_bytes,
        vm_hwm_kib().map_or_else(|| "unavailable".to_string(), |value| value.to_string())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "release concurrent append benchmark"]
async fn release_concurrent_append_latency() {
    let stream = BenchmarkStream::new();
    let expected_lines = input_line_count(CONCURRENT_INPUT_BYTES);
    let reader_enabled = !matches!(std::env::var("ORDERBOOK_BENCH_CONCURRENT_READER").as_deref(), Ok("0" | "false"));
    let done = Arc::new(AtomicBool::new(false));
    let reader_done = Arc::clone(&done);
    let (senders, mut receiver) = file_event_channels(&[EventSource::OrderDiffs], 16_384);
    let mut reader = FileReader::new(stream.base_dir.clone());
    reader.start_tracking(&stream.path);
    let sink = FileLineSink::Events { source: EventSource::OrderDiffs, tx: senders.into_iter().next().unwrap() };
    let path = stream.path.clone();
    let started = Instant::now();

    let producer = reader_enabled.then(|| {
        tokio::task::spawn_blocking(move || {
            let mut total = ProducerStats {
                raw_bytes: 0,
                returned_bytes: 0,
                lines: 0,
                largest_read_bytes: 0,
                read_calls: 0,
                batches_submitted: 0,
            };
            loop {
                let done_before_read = reader_done.load(Ordering::Acquire);
                let (progress, read) = read_and_submit(&mut reader, &sink);
                add_stats(&mut total, read);
                if progress == ReadProgress::Idle {
                    if done_before_read {
                        break;
                    }
                    thread::yield_now();
                }
            }
            drop(sink);
            total
        })
    });

    let writer = tokio::task::spawn_blocking(move || {
        let writer_started = Instant::now();
        let file = OpenOptions::new().append(true).open(path).expect("benchmark stream file should open");
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        let mut bytes = 0u64;
        let mut longest_flush = Duration::ZERO;
        for index in 0..expected_lines {
            let line = benchmark_line(index);
            writer.write_all(line.as_bytes()).expect("benchmark line should append");
            writer.write_all(b"\n").expect("benchmark newline should append");
            bytes += line.len() as u64 + 1;
            if index % 256 == 255 || index + 1 == expected_lines {
                let flush_started = Instant::now();
                writer.flush().expect("benchmark batch should flush");
                longest_flush = longest_flush.max(flush_started.elapsed());
            }
        }
        done.store(true, Ordering::Release);
        (bytes, longest_flush, writer_started.elapsed())
    });

    let drained_lines = if reader_enabled {
        tokio::time::timeout(TIMEOUT, drain_order_diffs(&mut receiver))
            .await
            .expect("concurrent benchmark pipeline exceeded its time bound")
    } else {
        drop(receiver);
        0
    };
    let (input_bytes, longest_flush, writer_elapsed) = writer.await.expect("benchmark writer should finish");
    let producer = match producer {
        Some(producer) => Some(producer.await.expect("benchmark producer should finish")),
        None => None,
    };
    let elapsed = started.elapsed();

    assert_eq!(std::fs::metadata(&stream.path).unwrap().len(), input_bytes);
    if let Some(producer) = &producer {
        assert_eq!(producer.raw_bytes, input_bytes);
        assert_eq!(producer.returned_bytes, input_bytes);
        assert_eq!(producer.lines, expected_lines);
        assert_eq!(drained_lines, expected_lines);
    } else {
        let contents = std::fs::read_to_string(&stream.path).expect("benchmark output should be readable");
        assert_eq!(contents.lines().count() as u64, expected_lines);
        assert_eq!(contents.lines().last(), Some(benchmark_line(expected_lines - 1).as_str()));
    }

    let mib_per_second = input_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    println!(
        "reader_enabled={reader_enabled} bytes={input_bytes} lines={expected_lines} elapsed={elapsed:?} writer_elapsed={writer_elapsed:?} throughput_mib_s={mib_per_second:.2} longest_append_flush={longest_flush:?} read_calls={} batches_submitted={} largest_read_return_bytes={} vm_hwm_kib={}",
        producer.as_ref().map_or(0, |stats| stats.read_calls),
        producer.as_ref().map_or(0, |stats| stats.batches_submitted),
        producer.as_ref().map_or(0, |stats| stats.largest_read_bytes),
        vm_hwm_kib().map_or_else(|| "unavailable".to_string(), |value| value.to_string())
    );
}

#[test]
#[ignore = "release notification callback benchmark"]
fn release_notification_storm() {
    const NOTIFICATIONS: u32 = 5_000_000;
    for sample in 1..=3 {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let signal = Arc::new(WatcherSignal { flags: AtomicU8::new(0), tx });
        let callback_signal = Arc::clone(&signal);
        let elapsed = thread::spawn(move || {
            let started = Instant::now();
            for _ in 0..NOTIFICATIONS {
                callback_signal.notify(std::hint::black_box(NOTIFY_DATA));
            }
            started.elapsed()
        })
        .join()
        .unwrap();
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
        assert_eq!(signal.take(), NOTIFY_DATA);
        println!(
            "sample={sample} notifications={NOTIFICATIONS} elapsed={elapsed:?} ns_per_notification={:.2}",
            elapsed.as_secs_f64() * 1e9 / f64::from(NOTIFICATIONS)
        );
    }
}
