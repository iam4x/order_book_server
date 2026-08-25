use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use rustc_hash::FxHashMap;

use crate::{
    listeners::order_book::{L2Snapshots, TimedSnapshots},
    order_book::{
        Coin, InnerOrder, Oid, QueuePlacement, RawBbo, Sz,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};

pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    ignore_spot: bool,
    pending_order_statuses: FxHashMap<Oid, Pending<NodeDataOrderStatus>>,
    pending_new_diffs: FxHashMap<Oid, Pending<PendingNewDiff>>,
    order_status_height: u64,
    order_diff_height: u64,
    repair_reasons: HashSet<PendingRepairReason>,
}

const PENDING_MAX_AGE: Duration = Duration::from_secs(60);
const MAX_PENDING_ORDER_STATUSES: usize = 50_000;
const MAX_PENDING_NEW_DIFFS: usize = 10_000;

struct Pending<T> {
    payload: T,
    first_seen: Instant,
}

#[derive(Clone)]
struct PendingNewDiff {
    coin: Coin,
    sz: Sz,
    insert_before: Option<Oid>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum PendingRepairReason {
    ExpiredStatus,
    ExpiredDiff,
    StatusCapacity,
    DiffCapacity,
    CoinMismatch,
}

impl PendingRepairReason {
    pub(super) const fn metric_label(self) -> &'static str {
        match self {
            Self::ExpiredStatus => "expired_status",
            Self::ExpiredDiff => "expired_diff",
            Self::StatusCapacity => "status_capacity",
            Self::DiffCapacity => "diff_capacity",
            Self::CoinMismatch => "coin_mismatch",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum OrderDiffApplyMode {
    Live,
    Replay,
}

fn record_insert_before_placement(placement: QueuePlacement) {
    match placement {
        QueuePlacement::Honored => crate::metrics::INSERT_BEFORE_HONORED_TOTAL.inc(),
        QueuePlacement::Fallback => crate::metrics::INSERT_BEFORE_FALLBACK_TOTAL.inc(),
        QueuePlacement::NotApplicable => {}
    }
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            pending_order_statuses: FxHashMap::default(),
            pending_new_diffs: FxHashMap::default(),
            order_status_height: height,
            order_diff_height: height,
            repair_reasons: HashSet::new(),
        }
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    pub(super) const fn time(&self) -> u64 {
        self.time
    }

    pub(super) const fn order_status_height(&self) -> u64 {
        self.order_status_height
    }

    pub(super) const fn order_diff_height(&self) -> u64 {
        self.order_diff_height
    }

    pub(super) fn set_time(&mut self, time: u64) {
        self.time = time;
    }

    // forcibly take snapshot - (time, height, snapshot)
    pub(super) fn compute_snapshot(&self) -> TimedSnapshots {
        TimedSnapshots { time: self.time, height: self.height, snapshot: self.order_book.to_snapshots_par() }
    }

    /// Incremental variant: rebuilds variants only for `changed_coins` and reuses
    /// cached Arc'd entries for every other coin. The caller owns the cache so
    /// the borrow on `&self` here only touches the order book.
    pub(super) fn l2_snapshots_incremental(
        &self,
        changed_coins: &HashSet<Coin>,
        requested_params: &HashSet<crate::listeners::order_book::L2SnapshotParams>,
        cache: &mut HashMap<
            Coin,
            std::sync::Arc<
                HashMap<
                    crate::listeners::order_book::L2SnapshotParams,
                    crate::order_book::Snapshot<crate::types::inner::InnerLevel>,
                >,
            >,
        >,
    ) -> (u64, L2Snapshots) {
        let snapshots = crate::listeners::order_book::utils::compute_l2_snapshots_incremental(
            &self.order_book,
            changed_coins,
            requested_params,
            cache,
        );
        (self.time, snapshots)
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    /// Count of OrderStatuses waiting for their OrderDiff::New to arrive
    pub(super) fn pending_order_statuses_count(&self) -> usize {
        self.pending_order_statuses.len()
    }

    /// Count of OrderDiff::New sizes waiting for their OrderStatus to arrive  
    pub(super) fn pending_new_diffs_count(&self) -> usize {
        self.pending_new_diffs.len()
    }

    /// Total number of orders currently in the orderbook
    pub(super) fn order_count(&self) -> usize {
        self.order_book.order_count()
    }

    /// Number of coins tracked in the orderbook
    pub(super) fn coin_count(&self) -> usize {
        self.order_book.as_ref().len()
    }

    pub(super) fn cleanup_stale_pending(&mut self) {
        self.cleanup_stale_pending_at(Instant::now());
    }

    fn cleanup_stale_pending_at(&mut self, now: Instant) {
        let old_statuses = self.pending_order_statuses.len();
        self.pending_order_statuses
            .retain(|_, pending| now.saturating_duration_since(pending.first_seen) <= PENDING_MAX_AGE);
        let expired_statuses = old_statuses - self.pending_order_statuses.len();
        if expired_statuses > 0 {
            log::warn!("Expired {expired_statuses} unmatched order statuses after 60 seconds");
            self.repair_reasons.insert(PendingRepairReason::ExpiredStatus);
        }

        let old_diffs = self.pending_new_diffs.len();
        self.pending_new_diffs
            .retain(|_, pending| now.saturating_duration_since(pending.first_seen) <= PENDING_MAX_AGE);
        let expired_diffs = old_diffs - self.pending_new_diffs.len();
        if expired_diffs > 0 {
            log::warn!("Expired {expired_diffs} unmatched new-order diffs after 60 seconds");
            self.repair_reasons.insert(PendingRepairReason::ExpiredDiff);
        }
    }

    pub(super) fn compact_order_book(&mut self) {
        let compacted = self.order_book.compact_all();
        if compacted > 0 {
            let (live, cap) = self.order_book.slab_stats();
            log::info!("Compacted {compacted} price-level slabs (live={live}, capacity={cap})");
        }
    }

    pub(super) fn take_repair_reasons(&mut self) -> HashSet<PendingRepairReason> {
        std::mem::take(&mut self.repair_reasons)
    }

    fn insert_pending_status(&mut self, oid: Oid, status: NodeDataOrderStatus) {
        if let Some(pending) = self.pending_order_statuses.get_mut(&oid) {
            pending.payload = status;
            return;
        }
        if self.pending_order_statuses.len() >= MAX_PENDING_ORDER_STATUSES {
            log::warn!(
                "Pending order-status cache reached {MAX_PENDING_ORDER_STATUSES} entries; requesting snapshot repair"
            );
            self.pending_order_statuses = FxHashMap::default();
            self.repair_reasons.insert(PendingRepairReason::StatusCapacity);
        }
        self.pending_order_statuses.insert(oid, Pending { payload: status, first_seen: Instant::now() });
    }

    fn insert_pending_diff(&mut self, oid: Oid, diff: PendingNewDiff) {
        if let Some(pending) = self.pending_new_diffs.get_mut(&oid) {
            pending.payload = diff;
            return;
        }
        if self.pending_new_diffs.len() >= MAX_PENDING_NEW_DIFFS {
            log::warn!("Pending new-diff cache reached {MAX_PENDING_NEW_DIFFS} entries; requesting snapshot repair");
            self.pending_new_diffs = FxHashMap::default();
            self.repair_reasons.insert(PendingRepairReason::DiffCapacity);
        }
        self.pending_new_diffs.insert(oid, Pending { payload: diff, first_seen: Instant::now() });
    }

    /// Get BBO for specific coins only - even faster for selective broadcast
    /// Only computes BBO for coins that changed, avoiding iteration over all 150+ coins
    pub(super) fn get_bbos_for_coins(&self, coins: &HashSet<Coin>) -> (u64, HashMap<Coin, RawBbo>) {
        let bbos = self.order_book.get_bbos_for_coins(coins);
        (self.time, bbos)
    }

    pub(super) fn get_bbos_for_changed_coins(&self, coins: &HashSet<Coin>) -> (u64, Vec<(Coin, RawBbo)>) {
        let bbos = self.order_book.get_bbos_for_changed_coins(coins);
        (self.time, bbos)
    }

    pub(super) fn get_all_bbos(&self) -> (u64, Vec<(Coin, RawBbo)>) {
        let bbos = self.order_book.get_all_bbos();
        (self.time, bbos)
    }

    /// HFT-specific: Process OrderStatuses independently without block synchronization
    /// Uses bidirectional caching - if diff already arrived, add order immediately
    /// Returns the set of coins that were modified (for selective BBO broadcast)
    pub(super) fn apply_order_statuses_hft(&mut self, batch: Batch<NodeDataOrderStatus>) -> Result<HashSet<Coin>> {
        let height = batch.block_number();
        let time = batch.block_time();
        let mut changed_coins = HashSet::new();
        self.order_status_height = self.order_status_height.max(height);

        // Update height/time to track progress (>= ensures time updates even at same height)
        if height >= self.height {
            self.height = height;
            self.time = time;
        }

        for order_status in batch.events() {
            let oid = Oid::new(order_status.order.oid);
            let order_coin = Coin::new(&order_status.order.coin);
            if (order_coin.is_spot() && self.ignore_spot) || !order_status.is_inserted_into_book() {
                continue;
            }

            if let Some(pending_diff) = self.pending_new_diffs.get(&oid) {
                if pending_diff.payload.coin != order_coin {
                    let diff_coin = pending_diff.payload.coin.clone();
                    self.pending_new_diffs.remove(&oid);
                    self.repair_reasons.insert(PendingRepairReason::CoinMismatch);
                    log::warn!(
                        "Rejecting mismatched order halves for oid={oid:?}: status coin={order_coin:?}, diff coin={diff_coin:?}"
                    );
                    continue;
                }

                let pending_diff = pending_diff.payload.clone();
                let time = order_status.time.and_utc().timestamp_millis();
                let mut inner_order: InnerL4Order = order_status.try_into()?;
                inner_order.modify_sz(pending_diff.sz);
                inner_order.convert_trigger(u64::try_from(time).unwrap_or_default());
                self.pending_new_diffs.remove(&oid);
                record_insert_before_placement(
                    self.order_book.add_order_before(inner_order, pending_diff.insert_before),
                );
                changed_coins.insert(order_coin.clone());
                log::debug!("Order added (status arrived after diff): oid={:?} coin={:?}", oid, order_coin);
            } else {
                // Diff hasn't arrived yet - cache the OrderStatus
                self.insert_pending_status(oid, order_status);
            }
        }
        Ok(changed_coins)
    }

    #[cfg(test)]
    pub(crate) fn pending_order_statuses_has(&self, oid: &Oid) -> bool {
        self.pending_order_statuses.contains_key(oid)
    }

    #[cfg(test)]
    pub(crate) fn pending_new_diffs_has(&self, oid: &Oid) -> bool {
        self.pending_new_diffs.contains_key(oid)
    }

    /// HFT-specific: Process OrderDiffs independently without block synchronization
    /// Uses bidirectional caching - if status already arrived, add order immediately
    /// Returns the set of coins that were modified (for selective BBO broadcast)
    pub(super) fn apply_order_diffs_hft(&mut self, batch: Batch<NodeDataOrderDiff>) -> Result<HashSet<Coin>> {
        self.apply_order_diffs_hft_inner(batch, OrderDiffApplyMode::Live)
    }

    pub(super) fn replay_order_diffs_hft(&mut self, batch: Batch<NodeDataOrderDiff>) -> Result<HashSet<Coin>> {
        self.apply_order_diffs_hft_inner(batch, OrderDiffApplyMode::Replay)
    }

    fn apply_order_diffs_hft_inner(
        &mut self,
        batch: Batch<NodeDataOrderDiff>,
        mode: OrderDiffApplyMode,
    ) -> Result<HashSet<Coin>> {
        let height = batch.block_number();
        let time = batch.block_time();
        let mut changed_coins = HashSet::new();
        self.order_diff_height = self.order_diff_height.max(height);

        // Update height/time to track progress (>= ensures time updates even at same height)
        if height >= self.height {
            self.height = height;
            self.time = time;
        }

        for diff in batch.events() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz, insert_before } => {
                    // Check if OrderStatus already arrived
                    if let Some(pending_order) = self.pending_order_statuses.get(&oid) {
                        let order_coin = Coin::new(&pending_order.payload.order.coin);
                        if order_coin != coin {
                            self.pending_order_statuses.remove(&oid);
                            self.repair_reasons.insert(PendingRepairReason::CoinMismatch);
                            log::warn!(
                                "Rejecting mismatched order halves for oid={oid:?}: status coin={order_coin:?}, diff coin={coin:?}"
                            );
                            continue;
                        }

                        let time = pending_order.payload.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = pending_order.payload.clone().try_into()?;
                        inner_order.modify_sz(sz);
                        inner_order.convert_trigger(u64::try_from(time).unwrap_or_default());
                        self.pending_order_statuses.remove(&oid);
                        record_insert_before_placement(self.order_book.add_order_before(inner_order, insert_before));
                        changed_coins.insert(order_coin.clone());
                        log::debug!("Order added (diff arrived after status): oid={:?} coin={:?}", oid, order_coin);
                    } else {
                        self.insert_pending_diff(oid.clone(), PendingNewDiff { coin, sz, insert_before });
                    }
                }
                InnerOrderDiff::Update { orig_sz, new_sz } => {
                    if mode == OrderDiffApplyMode::Live {
                        let _ = self.order_book.modify_sz(oid, coin.clone(), new_sz);
                        changed_coins.insert(coin);
                    } else {
                        match self.order_book.order_sz(&oid, &coin) {
                            Some(current_sz) if current_sz == orig_sz => {
                                if self.order_book.modify_sz(oid, coin.clone(), new_sz) {
                                    changed_coins.insert(coin);
                                }
                            }
                            Some(current_sz) if current_sz == new_sz => {
                                log::debug!("Ignoring duplicate OrderDiff::Update for oid={oid:?} coin={coin:?}");
                            }
                            Some(current_sz) => {
                                log::debug!(
                                    "Skipping stale replayed OrderDiff::Update for oid={oid:?} coin={coin:?}: current \
                                     size {current_sz:?} did not match orig {orig_sz:?} or new {new_sz:?}"
                                );
                            }
                            None => {
                                log::debug!(
                                    "Skipping replayed OrderDiff::Update for unknown oid={oid:?} coin={coin:?}; order \
                                     is absent"
                                );
                            }
                        }
                    }
                }
                InnerOrderDiff::Remove => {
                    let _ = self.order_book.cancel_order(oid.clone(), coin.clone());
                    changed_coins.insert(coin);
                }
            }
        }
        Ok(changed_coins)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use chrono::NaiveDateTime;

    use super::*;
    use crate::{
        order_book::{Sz, multi_book::Snapshots},
        types::{L4Order, OrderDiff},
    };

    fn empty_state() -> OrderBookState {
        let snapshots = Snapshots::new(HashMap::new());
        OrderBookState::from_snapshot(snapshots, 0, 0, true, false)
    }

    fn make_l4_order_at(coin: &str, oid: u64, side: crate::order_book::types::Side, px: &str) -> L4Order {
        L4Order {
            user: None,
            coin: coin.to_string(),
            side,
            limit_px: px.to_string(),
            sz: "1.0".to_string(),
            oid,
            timestamp: 1000,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: "0.0".to_string(),
            children: Vec::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            orig_sz: "1.0".to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None,
        }
    }

    fn make_order_status(coin: &str, oid: u64, status: &str) -> NodeDataOrderStatus {
        make_order_status_at(coin, oid, status, crate::order_book::types::Side::Bid, "100.0")
    }

    fn make_order_status_at(
        coin: &str,
        oid: u64,
        status: &str,
        side: crate::order_book::types::Side,
        px: &str,
    ) -> NodeDataOrderStatus {
        NodeDataOrderStatus {
            time: NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            user: Address::new([0; 20]),
            hash: Some("0xabc".to_string()),
            builder: None,
            status: status.to_string(),
            order: make_l4_order_at(coin, oid, side, px),
        }
    }

    fn make_order_diff(coin: &str, oid: u64, diff: OrderDiff) -> NodeDataOrderDiff {
        serde_json::from_value(serde_json::json!({
            "user": "0x0000000000000000000000000000000000000000",
            "oid": oid,
            "px": "100.0",
            "coin": coin,
            "raw_book_diff": diff
        }))
        .unwrap()
    }

    fn make_status_batch(statuses: Vec<NodeDataOrderStatus>) -> Batch<NodeDataOrderStatus> {
        make_status_batch_at(statuses, 100)
    }

    fn make_status_batch_at(statuses: Vec<NodeDataOrderStatus>, block_number: u64) -> Batch<NodeDataOrderStatus> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": block_number,
            "events": statuses
        }))
        .unwrap()
    }

    fn make_diff_batch(diffs: Vec<NodeDataOrderDiff>) -> Batch<NodeDataOrderDiff> {
        make_diff_batch_at(diffs, 100)
    }

    fn make_diff_batch_at(diffs: Vec<NodeDataOrderDiff>, block_number: u64) -> Batch<NodeDataOrderDiff> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": block_number,
            "events": diffs
        }))
        .unwrap()
    }

    fn snapshot_oids(state: &OrderBookState, coin: &str, side: crate::order_book::types::Side) -> Vec<u64> {
        let snapshots = state.compute_snapshot().snapshot.value();
        let snapshot = snapshots.get(&Coin::new(coin)).expect("coin snapshot");
        let index = match side {
            crate::order_book::types::Side::Bid => 0,
            crate::order_book::types::Side::Ask => 1,
        };
        snapshot.as_ref()[index].iter().map(|order| order.oid).collect()
    }

    // ==================== Initialization Tests ====================

    #[test]
    fn test_from_snapshot_empty() {
        let state = empty_state();
        assert_eq!(state.height(), 0);
        assert_eq!(state.time(), 0);
        assert_eq!(state.order_count(), 0);
        assert_eq!(state.coin_count(), 0);
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    // ==================== Bidirectional Cache: Status First ====================

    #[test]
    fn test_status_first_then_diff_adds_order() {
        let mut state = empty_state();

        // 1. OrderStatus arrives first → cached
        let status = make_order_status("BTC", 42, "open");
        let batch = make_status_batch(vec![status]);
        let changed = state.apply_order_statuses_hft(batch).unwrap();
        assert!(changed.is_empty()); // not added yet
        assert_eq!(state.pending_order_statuses_count(), 1);
        assert!(state.pending_order_statuses_has(&Oid::new(42)));

        // 2. OrderDiff::New arrives → order added immediately
        let diff = make_order_diff("BTC", 42, OrderDiff::New { sz: "1.5".to_string(), insert_before: None });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.pending_order_statuses_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    #[test]
    fn test_status_first_then_diff_honors_insert_before() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![
                make_order_status("BTC", 1, "open"),
                make_order_status("BTC", 2, "open"),
            ]))
            .unwrap();
        state
            .apply_order_diffs_hft(make_diff_batch(vec![
                make_order_diff("BTC", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }),
                make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }),
            ]))
            .unwrap();

        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 3, "open")])).unwrap();
        state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff(
                "BTC",
                3,
                OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(2) },
            )]))
            .unwrap();

        assert_eq!(snapshot_oids(&state, "BTC", crate::order_book::types::Side::Bid), vec![1, 3, 2]);
    }

    // ==================== Bidirectional Cache: Diff First ====================

    #[test]
    fn test_diff_first_then_status_adds_order() {
        let mut state = empty_state();

        // 1. OrderDiff::New arrives first → size cached
        let diff = make_order_diff("ETH", 99, OrderDiff::New { sz: "2.0".to_string(), insert_before: None });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.is_empty()); // not added yet
        assert_eq!(state.pending_new_diffs_count(), 1);
        assert!(state.pending_new_diffs_has(&Oid::new(99)));

        // 2. OrderStatus arrives → order added immediately
        let status = make_order_status("ETH", 99, "open");
        let batch = make_status_batch(vec![status]);
        let changed = state.apply_order_statuses_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("ETH")));
        assert_eq!(state.pending_new_diffs_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    #[test]
    fn test_diff_first_then_status_honors_insert_before() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![
                make_order_status("ETH", 1, "open"),
                make_order_status("ETH", 2, "open"),
            ]))
            .unwrap();
        state
            .apply_order_diffs_hft(make_diff_batch(vec![
                make_order_diff("ETH", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }),
                make_order_diff("ETH", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }),
            ]))
            .unwrap();

        state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff(
                "ETH",
                3,
                OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(2) },
            )]))
            .unwrap();
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("ETH", 3, "open")])).unwrap();

        assert_eq!(snapshot_oids(&state, "ETH", crate::order_book::types::Side::Bid), vec![1, 3, 2]);
    }

    #[test]
    fn test_non_insertable_status_does_not_consume_pending_diff() {
        let mut state = empty_state();
        let diff = make_order_diff("BTC", 99, OrderDiff::New { sz: "2.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        let status = make_order_status("BTC", 99, "filled");
        let changed = state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();

        assert!(changed.is_empty());
        assert_eq!(state.order_count(), 0);
        assert!(state.pending_new_diffs_has(&Oid::new(99)));
    }

    #[test]
    fn test_pairs_across_fourteen_block_source_skew_in_either_order() {
        let mut state = empty_state();
        let status_first = make_order_status("BTC", 100, "open");
        state.apply_order_statuses_hft(make_status_batch_at(vec![status_first], 1_000)).unwrap();
        let matching_diff = make_order_diff("BTC", 100, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch_at(vec![matching_diff], 1_014)).unwrap();

        let diff_first = make_order_diff("ETH", 101, OrderDiff::New { sz: "2.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch_at(vec![diff_first], 2_014)).unwrap();
        let matching_status = make_order_status("ETH", 101, "open");
        state.apply_order_statuses_hft(make_status_batch_at(vec![matching_status], 2_000)).unwrap();

        assert_eq!(state.order_count(), 2);
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.pending_new_diffs_count(), 0);
        assert_eq!(state.order_status_height(), 2_000);
        assert_eq!(state.order_diff_height(), 2_014);
    }

    // ==================== OrderDiff Update/Remove ====================

    #[test]
    fn test_diff_update_changes_coin() {
        let mut state = empty_state();
        // First add an order via the bidirectional path
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.order_count(), 1);

        // Now send Update
        let update =
            make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "3.0".to_string() });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![update])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
    }

    #[test]
    fn test_live_diff_update_with_mismatched_orig_size_still_applies() {
        let mut state = empty_state();
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        let update =
            make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "3.0".to_string(), new_sz: "4.0".to_string() });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![update])).unwrap();

        let coins = HashSet::from([Coin::new("BTC")]);
        let (_time, bbos) = state.get_bbos_for_coins(&coins);
        let bid_sz = bbos.get(&Coin::new("BTC")).expect("BTC bbo").0.as_ref().expect("bid").1;
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(bid_sz, Sz::parse_from_str("4.0").unwrap());
    }

    #[test]
    fn test_replay_diff_update_with_mismatched_orig_size_is_ignored() {
        let mut state = empty_state();
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        let stale_update =
            make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "3.0".to_string(), new_sz: "4.0".to_string() });
        let changed = state.replay_order_diffs_hft(make_diff_batch(vec![stale_update])).unwrap();

        let coins = HashSet::from([Coin::new("BTC")]);
        let (_time, bbos) = state.get_bbos_for_coins(&coins);
        let bid_sz = bbos.get(&Coin::new("BTC")).expect("BTC bbo").0.as_ref().expect("bid").1;
        assert!(changed.is_empty());
        assert_eq!(bid_sz, Sz::parse_from_str("5.0").unwrap());
    }

    #[test]
    fn test_diff_remove_changes_coin() {
        let mut state = empty_state();
        // Add order
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        // Remove
        let remove = make_order_diff("BTC", 1, OrderDiff::Remove);
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![remove])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.order_count(), 0);
    }

    // ==================== Status Filtering ====================

    #[test]
    fn test_non_insertable_status_not_cached() {
        let mut state = empty_state();
        // "filled" status should NOT be cached
        let status = make_order_status("BTC", 42, "filled");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_ioc_not_cached() {
        let mut state = empty_state();
        let mut status = make_order_status("BTC", 42, "open");
        status.order.tif = Some("Ioc".to_string());
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    // ==================== Spot Filtering ====================

    #[test]
    fn test_spot_filtered_when_ignore_spot() {
        let snapshots = Snapshots::new(HashMap::new());
        let mut state = OrderBookState::from_snapshot(snapshots, 0, 0, true, true); // ignore_spot=true

        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert!(changed.is_empty());
        assert_eq!(state.pending_new_diffs_count(), 0); // skipped entirely
    }

    #[test]
    fn test_spot_status_filtered_when_ignore_spot() {
        let snapshots = Snapshots::new(HashMap::new());
        let mut state = OrderBookState::from_snapshot(snapshots, 0, 0, true, true);
        let status = make_order_status("@1", 1, "open");

        let changed = state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();

        assert!(changed.is_empty());
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_spot_not_filtered_when_not_ignoring() {
        let mut state = empty_state(); // ignore_spot=false
        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.pending_new_diffs_count(), 1); // cached
    }

    // ==================== Height/Time Tracking ====================

    #[test]
    fn test_height_updates_on_higher_block() {
        let mut state = empty_state();
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 500,
            "events": []
        }))
        .unwrap();
        state.apply_order_diffs_hft(batch).unwrap();
        assert_eq!(state.height(), 500);
    }

    #[test]
    fn test_height_not_downgraded() {
        let mut state = empty_state();
        // Set height to 500
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:31:00.000000000",
            "block_time": "2024-01-15T10:31:00.000000000",
            "block_number": 500,
            "events": []
        }))
        .unwrap();
        state.apply_order_diffs_hft(batch).unwrap();

        // Try to go to 200
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 200,
            "events": []
        }))
        .unwrap();
        state.apply_order_diffs_hft(batch).unwrap();
        assert_eq!(state.height(), 500); // unchanged
    }

    // ==================== Cleanup Tests ====================

    #[test]
    fn test_source_skew_does_not_drop_pending_order_statuses() {
        let mut state = empty_state();
        let statuses = (0..10_001u64).map(|oid| make_order_status("BTC", oid, "open")).collect();
        state.apply_order_statuses_hft(make_status_batch(statuses)).unwrap();
        assert!(state.pending_order_statuses_count() > 10_000);

        state.cleanup_stale_pending();

        let diffs = (0..10_001u64)
            .map(|oid| make_order_diff("BTC", oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }))
            .collect();
        state.apply_order_diffs_hft(make_diff_batch(diffs)).unwrap();

        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.order_count(), 10_001);
    }

    #[test]
    fn test_source_skew_does_not_drop_pending_new_diffs() {
        let mut state = empty_state();
        let diffs = (0..1_001u64)
            .map(|oid| make_order_diff("BTC", oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }))
            .collect();
        state.apply_order_diffs_hft(make_diff_batch(diffs)).unwrap();
        assert!(state.pending_new_diffs_count() > 1_000);

        state.cleanup_stale_pending();

        let statuses = (0..1_001u64).map(|oid| make_order_status("BTC", oid, "open")).collect();
        state.apply_order_statuses_hft(make_status_batch(statuses)).unwrap();

        assert_eq!(state.pending_new_diffs_count(), 0);
        assert_eq!(state.order_count(), 1_001);
    }

    #[test]
    fn test_cleanup_below_threshold_no_op() {
        let mut state = empty_state();
        for i in 0..100u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        state.cleanup_stale_pending();
        assert_eq!(state.pending_order_statuses_count(), 100); // not cleared
    }

    #[test]
    fn expired_pending_halves_request_repair() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
        state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff(
                "ETH",
                2,
                OrderDiff::New { sz: "1.0".to_string(), insert_before: None },
            )]))
            .unwrap();
        let now = Instant::now();
        let expired_at = now.checked_sub(PENDING_MAX_AGE + Duration::from_millis(1)).unwrap();
        state.pending_order_statuses.get_mut(&Oid::new(1)).unwrap().first_seen = expired_at;
        state.pending_new_diffs.get_mut(&Oid::new(2)).unwrap().first_seen = expired_at;

        state.cleanup_stale_pending_at(now);

        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.pending_new_diffs_count(), 0);
        assert_eq!(
            state.take_repair_reasons(),
            HashSet::from([PendingRepairReason::ExpiredStatus, PendingRepairReason::ExpiredDiff])
        );
    }

    #[test]
    fn duplicate_status_preserves_original_expiry_age() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
        let now = Instant::now();
        state.pending_order_statuses.get_mut(&Oid::new(1)).unwrap().first_seen =
            now.checked_sub(PENDING_MAX_AGE + Duration::from_millis(1)).unwrap();

        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
        state.cleanup_stale_pending_at(now);

        assert_eq!(state.pending_order_statuses_count(), 0);
        assert!(state.take_repair_reasons().contains(&PendingRepairReason::ExpiredStatus));
    }

    #[test]
    fn duplicate_diff_preserves_original_expiry_age() {
        let mut state = empty_state();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        let now = Instant::now();
        state.pending_new_diffs.get_mut(&Oid::new(1)).unwrap().first_seen =
            now.checked_sub(PENDING_MAX_AGE + Duration::from_millis(1)).unwrap();

        let duplicate = make_order_diff("BTC", 1, OrderDiff::New { sz: "2.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![duplicate])).unwrap();
        state.cleanup_stale_pending_at(now);

        assert_eq!(state.pending_new_diffs_count(), 0);
        assert!(state.take_repair_reasons().contains(&PendingRepairReason::ExpiredDiff));
    }

    #[test]
    fn pending_diff_capacity_is_hard_and_requests_repair() {
        let mut state = empty_state();
        let diffs = (0..=MAX_PENDING_NEW_DIFFS as u64)
            .map(|oid| make_order_diff("BTC", oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None }))
            .collect();

        state.apply_order_diffs_hft(make_diff_batch(diffs)).unwrap();

        assert_eq!(state.pending_new_diffs_count(), 1);
        assert!(state.take_repair_reasons().contains(&PendingRepairReason::DiffCapacity));
    }

    #[test]
    fn pending_status_capacity_is_hard_and_requests_repair() {
        let mut state = empty_state();
        let statuses =
            (0..=MAX_PENDING_ORDER_STATUSES as u64).map(|oid| make_order_status("BTC", oid, "open")).collect();

        state.apply_order_statuses_hft(make_status_batch(statuses)).unwrap();

        assert_eq!(state.pending_order_statuses_count(), 1);
        assert!(state.take_repair_reasons().contains(&PendingRepairReason::StatusCapacity));
    }

    #[test]
    fn mismatched_pair_coin_is_rejected_and_requests_repair() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();

        let changed = state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff(
                "ETH",
                1,
                OrderDiff::New { sz: "1.0".to_string(), insert_before: None },
            )]))
            .unwrap();

        assert!(changed.is_empty());
        assert_eq!(state.order_count(), 0);
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert!(state.take_repair_reasons().contains(&PendingRepairReason::CoinMismatch));
    }

    // ==================== Performance Tests ====================

    #[test]
    fn test_apply_diffs_performance() {
        let mut state = empty_state();
        // Pre-populate with order statuses
        for i in 0..1000u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }

        // Time matching diffs arrival
        let start = Instant::now();
        for i in 0..1000u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / 1000;

        eprintln!(
            "[PERF] apply_order_diffs_hft: 1000 New diffs (with cached statuses): {:?} ({:?}/event)",
            elapsed, per_event
        );
        assert_eq!(state.order_count(), 1000);
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_apply_statuses_performance() {
        let mut state = empty_state();
        // Pre-populate with diffs
        for i in 0..1000u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }

        let start = Instant::now();
        for i in 0..1000u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / 1000;

        eprintln!(
            "[PERF] apply_order_statuses_hft: 1000 statuses (with cached diffs): {:?} ({:?}/event)",
            elapsed, per_event
        );
        assert_eq!(state.order_count(), 1000);
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    #[test]
    fn test_universe_computation() {
        let mut state = empty_state();
        // Add orders for multiple coins
        for (i, coin) in ["BTC", "ETH", "SOL"].iter().enumerate() {
            let status = make_order_status(coin, i as u64, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let universe = state.compute_universe();
        assert_eq!(universe.len(), 3);
        assert!(universe.contains(&Coin::new("BTC")));
        assert!(universe.contains(&Coin::new("ETH")));
        assert!(universe.contains(&Coin::new("SOL")));
    }
}
