# File-reader performance

Measured on 2026-09-16 in the local development environment. The baseline was revision `0f8c1e4` with the benchmark fixture added before the implementation changed. These results measure file reads, line framing, channel transfer, height extraction, and exact ordered consumption. They exclude JSON event parsing, book updates, snapshots, and live `hl-node` behavior.

## Results

The fixture appends 67,108,932 bytes of generated NDJSON (565,808 records), then times the reader-to-consumer pipeline. Input generation occurs before the timer. File contents are cache-warm. Baseline has one sample; the new implementation has three, so baseline variance is unknown.

| Measurement | Before | After (median of 3) |
| --- | ---: | ---: |
| Throughput | 373.29 MiB/s | 437.52 MiB/s |
| Elapsed time | 171.45 ms | 146.28 ms |
| Process peak RSS (`VmHWM`) | 226,048 KiB | 42,024 KiB |
| Largest returned line payload | 67,108,932 bytes | 262,202 bytes |
| Channel line submissions | 565,808 individual lines | 257 batches |

Throughput increased 17.2%; process peak RSS fell 81.4%. The after runs use 258 reader calls, including the final EOF probe. The payload count includes records completed from a previous chunk, so it can exceed one 256 KiB read. RSS covers the entire test process and is not a production memory estimate. A final run after the recovery review recorded 447.45 MiB/s and 42,224 KiB peak RSS, with the same exact byte and record counts.

A separate 16 MiB concurrent-appender fixture delivered all 142,853 records in order. Across three runs, median writer time was 9.38 ms with the reader and 9.93 ms without it. Those short runs do not establish a production writer-latency guarantee. Pipeline elapsed time includes consumer drain and must not be interpreted as writer time. The original baseline did not record writer elapsed time.

The syscall trace included Cargo and the test harness. Total calls fell from 570,362 to 26,038; `futex` calls fell from 550,534 to 4,634. `read` calls rose from 4,705 to 5,057 as reads became bounded. Trace overhead substantially affected timing, so the throughput table uses untraced runs.

[Recorded run output](file-reader-results.tsv) contains each sample. The existing ingest checkpoint also passed after review at 462,203 lines/s for 81,200 lines, above its 81,300 lines/s release threshold. Its producer now supplies bounded batches concurrently rather than preloading a queue beyond the production byte limit.

## Reproduce

Run each command from the repository root. These benchmarks are ignored during the normal suite.

```sh
cargo test -p server --release release_file_reader_channel_throughput -- --ignored --nocapture
cargo test -p server --release release_concurrent_append_latency -- --ignored --nocapture
ORDERBOOK_BENCH_CONCURRENT_READER=0 cargo test -p server --release release_concurrent_append_latency -- --ignored --nocapture
cargo test -p server --release hft_ingest_throughput_checkpoint -- --nocapture
```

For a whole-process syscall summary:

```sh
strace -f -c -o /tmp/orderbook-reader-strace.txt cargo test -p server --release release_file_reader_channel_throughput -- --ignored --nocapture
```

Run without compilation or other concurrent load when comparing samples. The watcher integration test uses actual filesystem notifications, a one-batch channel, a burst, and hourly rotation. Separate tests cover reader backpressure while file appends continue, receiver shutdown, split UTF-8, oversized records, truncation, replacement, and predecessor drain.

## Bounds and resource use

Each source reads at most 256 KiB per call, retains at most 16 MiB of a partial record, and reserves at most 32 MiB for queued batch allocations and the merge cursor. A producer can hold one additional framed batch while waiting for queue capacity. Each consumer turn stops after 256 KiB or the record that crosses that threshold. String/vector capacity is charged; allocator metadata, channel storage, other book state, and snapshot work are outside that budget.

The callback retains one notification token and combines data/rescan/error flags. Normal reads use the held descriptor; pathname checks and discovery run on rescan hints or a one-second timer. Discovery skips hour directories older than the tracked day. A 10 ms data poll covers missed notifications. Streams are append-only within an inode; observed shrink and inode replacement trigger recovery. An in-place truncate and regrow beyond the previous offset between probes cannot be distinguished from append by this reader, as before.

Full queues park reader threads, leaving the remaining events in node files. No reader file lock or write blocks the node. CPU, memory bandwidth, page cache, disk reads, snapshots, and replay-journal writes still share host resources. Validate node block lag and writer latency on the deployment host under peak traffic; these local fixtures cannot prove that `hl-node` remains unaffected.
