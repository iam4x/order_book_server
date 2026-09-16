# PR 8 critical review

The audit reproduced four correctness failures and measured one callback inefficiency. Each fix has its own commit. The PR remains open and unmerged.

## Correctness findings

1. **Startup attachment could skip data.** A stream first created after watcher startup was attached at EOF, losing its existing records. Snapshot startup also relied on a five-second delay rather than completed watcher initialization. Watchers now acknowledge attachment before the snapshot scheduler starts. Only startup skips complete history; later files begin at byte zero. Attachment preserves an incomplete trailing record, finds files before empty day directories, and propagates initialization failure. The late-file test returned no records before the fix and returns the expected record afterward.

2. **Journal and queued updates used different snapshot boundaries.** A post-snapshot update could be ignored as stale while in the replay journal, despite applying normally after the snapshot swap. A regression retained size 5 instead of the expected size 4. Both paths now use `OrderBookState::apply_order_diffs_after_snapshot`; conservative replay guards apply only through the snapshot height.

3. **A fill record could make a large diff batch overflow the pending cache.** With the production source order, an older fill advanced the merge cursor so a 12,000-order diff batch preceded its matching status batch. The test reconstructed only 2,000 orders. The merge now prefers statuses at a new block height and alternates book sources afterward. The test reconstructs all 12,000 orders without a repair request; equal-height line alternation remains covered.

4. **Historical fills could starve book updates.** The receiver compared fill heights with book heights even though fills are independent of book reconstruction. The regression delivered only fills despite ready book records. Fills and book events now alternate delivery turns. Status/diff height ordering and quiet-source progress remain intact.

## Performance finding

A coalesced notification still attempted a channel send on every callback. The callback now sends only when flags transition from empty to pending, while accumulating rescan/error flags. The shared-thread callback benchmark fell from a median 24.83 ms to 13.47 ms for five million notifications, a 45.8% reduction. A regression covers new flags while the old token remains queued.

The filesystem design retains 256 KiB reads, a 32 MiB allocation budget per source, held read-only descriptors, bounded partial records, and bounded processor turns. A full downstream queue parks the reader and leaves the remaining events in node files. Tests cover appends while the reader is blocked, receiver shutdown, actual notifications and rotation, partial UTF-8, oversized records, replacement, and unreadable predecessors.

## Structure

The startup attachment phase is separate from ongoing reads; the live read path no longer decides whether to skip a first file. Snapshot replay policy has one owner in `OrderBookState`. Reader and queue regression tests moved into dedicated modules instead of pushing either production file past 1,000 lines. Production `parallel.rs` and `reader.rs` remain below 650 lines each.

## Verification and limits

`cargo fmt`, `cargo clippy --workspace --all-targets`, and `cargo test --workspace` pass. The suite has 372 passing tests and three ignored release benchmarks. Existing repository lint warnings remain; no watcher-specific Clippy warnings were introduced. [Recorded measurements](file-reader-results.tsv) include the callback comparison and final file/appender runs.

Final release runs delivered all 565,808 records in order at a median 440.15 MiB/s, with 42,348 KiB median peak RSS. The ingest checkpoint passed at 462,081 lines/s. Median writer elapsed was 10.31 ms with the reader and 10.02 ms without it across three runs each. That small observed difference does not establish zero writer overhead.

This is local verification, not a live Hypercore-node load test. Read-only access and reader-local backpressure avoid application-level file locks, but CPU, memory bandwidth, page cache, storage, snapshot computation, and journal writes still share host resources. The small cache-warm appender fixture does not certify production writer latency or blockchain synchronization.

Heightless CLI snapshots still use the existing visor-based replay cutoff. This audit verifies the cutoff/replay implementation, not an exact height contract for the deployed node binary. Upstream documents [height-bearing file snapshots through its info server](https://github.com/hyperliquid-dex/node#evm-and-info-servers), but moving snapshot work into the running node is outside this patch. Before deployment, compare reconstructed books against a snapshot with a known height and monitor node block lag and writer latency under peak traffic.
