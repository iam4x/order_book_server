use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use log::{error, info};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::process::Command;

use crate::{
    SnapshotMode,
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{Coin, Snapshot, multi_book::OrderBooks, types::InnerOrder},
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};

/// Configuration for snapshot fetching
#[derive(Debug, Clone)]
pub(super) struct SnapshotConfig {
    pub mode: SnapshotMode,
    pub docker_container: String,
    pub hlnode_binary: String,
    pub abci_state_path: Option<PathBuf>,
    pub snapshot_output_path: Option<PathBuf>,
    pub visor_state_path: Option<PathBuf>,
    pub data_dir: PathBuf,
}

pub(super) async fn process_rmp_file(config: &SnapshotConfig) -> Result<PathBuf> {
    info!("Triggering L4 snapshot via hl-node CLI (mode: {:?})...", config.mode);

    let (output_path, _visor_path) = match config.mode {
        SnapshotMode::Docker => {
            // Docker mode: run command inside container
            // data_dir should be the path containing node_*_by_block directories
            // Snapshot goes to parent of data_dir (sibling to "data" folder)
            let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
            let output_path = config.snapshot_output_path.clone().unwrap_or_else(|| parent_dir.join("snapshot.json"));
            let visor_path = config
                .visor_state_path
                .clone()
                .unwrap_or_else(|| parent_dir.join("hyperliquid_data/visor_abci_state.json"));

            let output = Command::new("docker")
                .args(&[
                    "exec",
                    &config.docker_container,
                    "./hl-node",
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    "hl/hyperliquid_data/abci_state.rmp",
                    "hl/snapshot.json",
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (docker mode)");
                }
                Err(e) => {
                    error!("Failed to execute docker command: {}", e);
                    return Err(e.into());
                }
            }

            (output_path, visor_path)
        }
        SnapshotMode::Direct => {
            // Direct mode: run hl-node directly on host
            let abci_path = config
                .abci_state_path
                .clone()
                .unwrap_or_else(|| config.data_dir.join("hl/hyperliquid_data/abci_state.rmp"));
            let output_path =
                config.snapshot_output_path.clone().unwrap_or_else(|| PathBuf::from("/tmp/hl_snapshot.json"));
            let visor_path = config
                .visor_state_path
                .clone()
                .unwrap_or_else(|| config.data_dir.join("hl/hyperliquid_data/visor_abci_state.json"));

            info!(
                "Running: {} --chain Mainnet compute-l4-snapshots --include-users {} {}",
                &config.hlnode_binary,
                abci_path.display(),
                output_path.display()
            );

            let output = Command::new(&config.hlnode_binary)
                .args(&[
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    abci_path.to_str().unwrap_or(""),
                    output_path.to_str().unwrap_or(""),
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        error!("stdout: {}", String::from_utf8_lossy(&out.stdout));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (direct mode)");
                }
                Err(e) => {
                    error!("Failed to execute hl-node command: {}", e);
                    return Err(e.into());
                }
            }

            (output_path, visor_path)
        }
    };

    // Verify file exists
    if output_path.exists() {
        info!("Snapshot file found at: {:?}", output_path);
        // Return tuple (output_path, visor_path) - but for now just output_path
        // The caller needs visor_path too, so we'll store it
        return Ok(output_path);
    }

    // Debug: List directory contents if file not found
    if let Some(parent) = output_path.parent() {
        error!("File not found. Listing directory {:?}:", parent);
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                error!(" - {:?}", entry.path());
            }
        } else {
            error!("Failed to read directory {:?}", parent);
        }
    }

    Err("Snapshot file not created".into())
}

/// Get the visor state path based on config
/// Get the visor state path based on config
/// data_dir should be the path containing node_*_by_block directories
/// visor_abci_state.json is in parent/hyperliquid_data/
pub(super) fn get_visor_path(config: &SnapshotConfig) -> PathBuf {
    config.visor_state_path.clone().unwrap_or_else(|| {
        let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
        parent_dir.join("hyperliquid_data/visor_abci_state.json")
    })
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

#[cfg(test)]
pub(super) fn all_l2_snapshot_params() -> HashSet<L2SnapshotParams> {
    let mut params = HashSet::with_capacity(7);
    params.insert(L2SnapshotParams::new(None, None));
    for n_sig_figs in (2..=5).rev() {
        if n_sig_figs == 5 {
            for mantissa in [None, Some(2), Some(5)] {
                params.insert(L2SnapshotParams::new(Some(n_sig_figs), mantissa));
            }
        } else {
            params.insert(L2SnapshotParams::new(Some(n_sig_figs), None));
        }
    }
    params
}

/// Build the seven L2 aggregation variants for a single coin's order book.
/// Pulled out of `compute_l2_snapshots` so incremental updates can call it
/// per coin without rerunning the full universe scan.
fn compute_l2_variants_for_coin<O: InnerOrder>(
    order_book: &crate::order_book::OrderBook<O>,
    requested_params: &HashSet<L2SnapshotParams>,
) -> HashMap<L2SnapshotParams, Snapshot<InnerLevel>> {
    let max_levels = Some(crate::types::subscription::MAX_LEVELS);
    let params: Vec<_> = requested_params.iter().copied().collect();
    let raw_params: Vec<_> = params.iter().map(|params| (params.n_sig_figs, params.mantissa)).collect();
    params.into_iter().zip(order_book.to_l2_snapshots(max_levels, &raw_params)).collect()
}

/// Incremental rebuild: recomputes variants only for `changed_coins`, reuses
/// the cached `Arc<HashMap>` for every other coin. Returns a fresh `L2Snapshots`
/// holding `Arc::clone`d entries — the outgoing broadcast message and the
/// listener-side cache share the underlying inner maps, so unchanged coins
/// cost a single Arc bump per broadcast instead of a full level-vector clone.
///
/// Also evicts cache entries for coins no longer present in `order_books`
/// (e.g. when a coin is delisted and the multi-book removes it). Without
/// this the cache would grow monotonically with the universe size.
pub(super) fn compute_l2_snapshots_incremental<O: InnerOrder + Send + Sync>(
    order_books: &OrderBooks<O>,
    changed_coins: &HashSet<Coin>,
    requested_params: &HashSet<L2SnapshotParams>,
    cache: &mut HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
) -> L2Snapshots {
    if requested_params.is_empty() {
        cache.clear();
        return L2Snapshots(HashMap::new());
    }

    // Evict stale entries.
    cache.retain(|coin, _| order_books.as_ref().contains_key(coin));

    // Determine which coins we actually need to (re)compute: anything in
    // `changed_coins` that the book still contains, plus any present-but-uncached
    // coins (first-time broadcast after a snapshot reset).
    let mut to_compute: HashSet<Coin> =
        changed_coins.iter().filter(|c| order_books.as_ref().contains_key(c)).cloned().collect();
    for coin in order_books.as_ref().keys() {
        let has_all_requested_params = cache.get(coin).is_some_and(|variants| {
            variants.len() == requested_params.len()
                && requested_params.iter().all(|params| variants.contains_key(params))
        });
        if !has_all_requested_params {
            to_compute.insert(coin.clone());
        }
    }

    // Parallel recompute for the coins we need.
    let updates: Vec<(Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>)> = to_compute
        .into_par_iter()
        .filter_map(|coin| {
            order_books
                .as_ref()
                .get(&coin)
                .map(|book| (coin, Arc::new(compute_l2_variants_for_coin(book, requested_params))))
        })
        .collect();
    let mut snapshot = HashMap::with_capacity(updates.len());
    for (coin, arc) in updates {
        cache.insert(coin.clone(), Arc::clone(&arc));
        snapshot.insert(coin, arc);
    }

    // Return only the entries that were recomputed for this update. The listener
    // cache still holds the full active state for future reuse.
    L2Snapshots(snapshot)
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use alloy::primitives::Address;

    use super::*;
    use crate::{
        order_book::{Px, Side, Sz, multi_book::Snapshots, types::InnerOrder},
        types::inner::InnerL4Order,
    };

    fn order(oid: u64, coin: &str, side: Side, sz: &str, px: &str) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(coin),
            side,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str(sz).unwrap(),
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        }
    }

    #[test]
    fn test_incremental_reuses_arc_for_unchanged_coins() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        // First call seeds the cache for both coins.
        let seeded = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_l2_snapshot_params(), &mut cache);
        assert_eq!(seeded.as_ref().len(), 2);
        assert_eq!(cache.len(), 2);
        let btc_first = Arc::clone(cache.get(&Coin::new("BTC")).unwrap());
        let eth_first = Arc::clone(cache.get(&Coin::new("ETH")).unwrap());

        // Mark BTC changed; ETH unchanged. ETH's Arc must be the same object.
        let changed: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        books.add_order(order(3, "BTC", Side::Bid, "2", "50001"));
        let changed_snapshot =
            compute_l2_snapshots_incremental(&books, &changed, &all_l2_snapshot_params(), &mut cache);
        assert_eq!(changed_snapshot.as_ref().len(), 1);
        assert!(changed_snapshot.as_ref().contains_key(&Coin::new("BTC")));

        let btc_after = cache.get(&Coin::new("BTC")).unwrap();
        let eth_after = cache.get(&Coin::new("ETH")).unwrap();
        assert!(!Arc::ptr_eq(&btc_first, btc_after), "BTC should have been recomputed");
        assert!(Arc::ptr_eq(&eth_first, eth_after), "ETH must be Arc-shared (not recomputed)");
    }

    #[test]
    fn test_incremental_evicts_removed_coins() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        drop(compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_l2_snapshot_params(), &mut cache));
        assert!(cache.contains_key(&Coin::new("BTC")));

        // Cancel BTC's only order — the multi-book evicts the empty book, which
        // means our cache must also drop the entry on the next incremental call.
        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC"));
        drop(compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_l2_snapshot_params(), &mut cache));
        assert!(!cache.contains_key(&Coin::new("BTC")), "BTC entry should have been evicted from the cache");
        assert!(cache.contains_key(&Coin::new("ETH")));
    }

    #[test]
    fn test_l2_variants_are_capped_to_subscription_max_levels() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        for i in 0..150u64 {
            books.add_order(order(i, "BTC", Side::Bid, "1", &(50_000 - i).to_string()));
        }

        let mut cache = HashMap::new();
        drop(compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_l2_snapshot_params(), &mut cache));
        let btc = cache.get(&Coin::new("BTC")).expect("BTC cache");

        for snapshot in btc.values() {
            assert!(snapshot.as_ref()[0].len() <= crate::types::subscription::MAX_LEVELS);
            assert!(snapshot.as_ref()[1].len() <= crate::types::subscription::MAX_LEVELS);
        }
        assert_eq!(
            btc.get(&L2SnapshotParams::new(None, None)).expect("default l2").as_ref()[0].len(),
            crate::types::subscription::MAX_LEVELS
        );
    }

    #[test]
    fn test_l2_variants_only_include_requested_params() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));

        let requested_params = HashSet::from([L2SnapshotParams::new(None, None)]);
        let mut cache = HashMap::new();
        drop(compute_l2_snapshots_incremental(&books, &HashSet::new(), &requested_params, &mut cache));

        let btc = cache.get(&Coin::new("BTC")).expect("BTC cache");
        assert_eq!(btc.len(), 1);
        assert!(btc.contains_key(&L2SnapshotParams::new(None, None)));
    }
}
