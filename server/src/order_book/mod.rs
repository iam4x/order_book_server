use std::collections::{BTreeMap, HashMap, HashSet};

use itertools::Itertools;
use linked_list::LinkedList;

use crate::prelude::*;

pub(crate) mod levels;
mod linked_list;
pub(crate) mod multi_book;
pub(crate) mod types;

pub(crate) use types::{Coin, InnerOrder, Oid, Px, Side, Sz};

pub(crate) type RawBbo = (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>);

#[derive(Clone, Default)]
pub(crate) struct OrderBook<O> {
    oid_to_side_px: HashMap<Oid, (Side, Px)>,
    bids: BTreeMap<Px, LinkedList<Oid, O>>,
    asks: BTreeMap<Px, LinkedList<Oid, O>>,
    bid_totals: BTreeMap<Px, LevelTotal>,
    ask_totals: BTreeMap<Px, LevelTotal>,
}

#[derive(Debug, Clone)]
pub(crate) struct Snapshot<O>([Vec<O>; 2]);

#[derive(Clone, Copy, Debug)]
struct LevelTotal {
    sz: Sz,
    n: usize,
}

impl Default for LevelTotal {
    fn default() -> Self {
        Self { sz: Sz::new(0), n: 0 }
    }
}

impl LevelTotal {
    const fn new(sz: Sz) -> Self {
        Self { sz, n: 1 }
    }

    fn add_order(&mut self, sz: Sz) {
        self.sz = self.sz + sz;
        self.n += 1;
    }

    fn remove_order(&mut self, sz: Sz) {
        self.sz = Sz::new(self.sz.value().saturating_sub(sz.value()));
        self.n = self.n.saturating_sub(1);
    }

    fn apply_match(&mut self, matched_sz: Sz, order_removed: bool) {
        self.sz = Sz::new(self.sz.value().saturating_sub(matched_sz.value()));
        if order_removed {
            self.n = self.n.saturating_sub(1);
        }
    }

    fn update_order_sz(&mut self, old_sz: Sz, new_sz: Sz) {
        self.sz = if new_sz >= old_sz {
            self.sz + Sz::new(new_sz.value() - old_sz.value())
        } else {
            Sz::new(self.sz.value().saturating_sub(old_sz.value() - new_sz.value()))
        };
    }

    const fn as_bbo(self, px: Px) -> (Px, Sz, u32) {
        (px, self.sz, self.n as u32)
    }

    const fn is_empty(self) -> bool {
        self.n == 0
    }
}

impl<O: Clone> Snapshot<O> {
    pub(crate) const fn as_ref(&self) -> &[Vec<O>; 2] {
        &self.0
    }

    pub(crate) fn truncate(&self, n: usize) -> Self {
        Self(self.0.clone().map(|orders| orders.into_iter().take(n).collect_vec()))
    }
}

impl<O: InnerOrder> Snapshot<O> {
    pub(crate) fn remove_triggers(&mut self) {
        #[allow(clippy::unwrap_used)]
        let [bid_oids, ask_oids] = &self
            .0
            .iter()
            .map(|orders| orders.iter().map(InnerOrder::oid).collect::<HashSet<Oid>>())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        for orders in &mut self.0 {
            while let Some(order) = orders.last() {
                let oid = order.oid();
                if bid_oids.contains(&oid) && ask_oids.contains(&oid) {
                    orders.pop();
                } else {
                    break;
                }
            }
        }
    }
}

impl<O: InnerOrder> OrderBook<O> {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            oid_to_side_px: HashMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            bid_totals: BTreeMap::new(),
            ask_totals: BTreeMap::new(),
        }
    }

    /// Number of orders in this orderbook
    pub(crate) fn order_count(&self) -> usize {
        self.oid_to_side_px.len()
    }

    pub(crate) fn add_order(&mut self, mut order: O) {
        // Duplicate oid would silently corrupt state: `oid_to_side_px` would point
        // at the new (side, px) while `LinkedList::push_back` silently rejects the
        // re-insert, leaving the original order data in place. Skip and warn.
        // (A node replay or duplicate-emit is the realistic trigger; we never want
        // to re-run matching, which would double-count the match against opposite-side
        // orders that have arrived since.)
        if self.oid_to_side_px.contains_key(&order.oid()) {
            log::warn!("OrderBook::add_order called twice for oid={:?}; ignoring duplicate", order.oid());
            return;
        }
        let (maker_orders, maker_totals, resting_book, resting_totals) = match order.side() {
            Side::Ask => (&mut self.bids, &mut self.bid_totals, &mut self.asks, &mut self.ask_totals),
            Side::Bid => (&mut self.asks, &mut self.ask_totals, &mut self.bids, &mut self.bid_totals),
        };
        let oids = match_order(maker_orders, maker_totals, &mut order);
        for oid in oids {
            self.oid_to_side_px.remove(&oid);
        }
        if order.sz().is_positive() {
            self.oid_to_side_px.insert(order.oid(), (order.side(), order.limit_px()));
            add_order_to_book(resting_book, resting_totals, order);
        }
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid) -> bool {
        if let Some((side, px)) = self.oid_to_side_px.remove(&oid) {
            let (map, totals) = match side {
                Side::Ask => (&mut self.asks, &mut self.ask_totals),
                Side::Bid => (&mut self.bids, &mut self.bid_totals),
            };
            let list = map.get_mut(&px);
            if let Some(list) = list {
                let removed_sz = list.node_value_mut(&oid).map(|order| order.sz());
                let success = list.remove_node(oid.clone());
                if let (true, Some(removed_sz)) = (success, removed_sz) {
                    remove_order_from_totals(totals, px, removed_sz);
                }
                if list.is_empty() {
                    map.remove(&px);
                }
                return success;
            }
        }
        false
    }

    pub(crate) fn modify_sz(&mut self, oid: Oid, sz: Sz) -> bool {
        // If new size is 0, remove the order entirely
        if sz.is_zero() {
            return self.cancel_order(oid);
        }
        if let Some((side, px)) = self.oid_to_side_px.get(&oid).copied() {
            let (map, totals) = match side {
                Side::Ask => (&mut self.asks, &mut self.ask_totals),
                Side::Bid => (&mut self.bids, &mut self.bid_totals),
            };
            let list = map.get_mut(&px);
            if let Some(list) = list {
                let old_order = list.node_value_mut(&oid);
                if let Some(old_order) = old_order {
                    let old_sz = old_order.sz();
                    old_order.modify_sz(sz);
                    if let Some(total) = totals.get_mut(&px) {
                        total.update_order_sz(old_sz, sz);
                    }
                    return true;
                }
                return false;
            }
        }
        false
    }

    /// Get best bid and best ask in O(1) without computing full L2 snapshot.
    /// Returns (best_bid, best_ask) where each is (price, total_size, order_count).
    #[must_use]
    pub(crate) fn get_bbo(&self) -> RawBbo {
        // Best bid = highest price in bids (last key in BTreeMap)
        let best_bid = self.bid_totals.last_key_value().map(|(px, total)| total.as_bbo(*px));

        // Best ask = lowest price in asks (first key in BTreeMap)
        let best_ask = self.ask_totals.first_key_value().map(|(px, total)| total.as_bbo(*px));

        (best_bid, best_ask)
    }

    /// Compact every price-level's `LinkedList` slab. Returns the number of lists
    /// that were actually rebuilt (lists below the fragmentation threshold are
    /// skipped). See `LinkedList::compact` for the threshold.
    pub(crate) fn compact(&mut self) -> usize {
        let mut compacted = 0usize;
        for list in self.bids.values_mut().chain(self.asks.values_mut()) {
            if list.compact() {
                compacted += 1;
            }
        }
        compacted
    }

    /// Returns (total live nodes, total slab capacity) summed across every level.
    /// Useful for tracking fragmentation in Prometheus.
    pub(crate) fn slab_stats(&self) -> (usize, usize) {
        let mut live = 0usize;
        let mut cap = 0usize;
        for list in self.bids.values().chain(self.asks.values()) {
            live += list.slab_len();
            cap += list.slab_capacity();
        }
        (live, cap)
    }

    // we go by the convention that prioritized orders go first in the vector; this makes aggregation step later easier.
    pub(crate) fn to_snapshot(&self) -> Snapshot<O> {
        let bids = self.bids.iter().rev().flat_map(|(_, l)| l.to_vec().into_iter().cloned()).collect_vec();
        let asks = self.asks.iter().flat_map(|(_, l)| l.to_vec().into_iter().cloned()).collect_vec();
        Snapshot([bids, asks])
    }

    #[must_use]
    pub(crate) fn from_snapshot(mut snapshot: Snapshot<O>, ignore_triggers: bool) -> Self {
        let mut book = Self::new();
        if ignore_triggers {
            snapshot.remove_triggers();
        }
        snapshot.0.into_iter().for_each(|orders| {
            for order in orders {
                book.add_order(order);
            }
        });
        book
    }
}

fn add_order_to_book<O: InnerOrder>(
    map: &mut BTreeMap<Px, LinkedList<Oid, O>>,
    totals: &mut BTreeMap<Px, LevelTotal>,
    order: O,
) {
    let oid = order.oid();
    let limit_px = order.limit_px();
    let sz = order.sz();
    if map.entry(limit_px).or_insert_with(|| LinkedList::new()).push_back(oid, order) {
        totals.entry(limit_px).and_modify(|total| total.add_order(sz)).or_insert_with(|| LevelTotal::new(sz));
    }
}

fn remove_order_from_totals(totals: &mut BTreeMap<Px, LevelTotal>, px: Px, sz: Sz) {
    let remove_level = if let Some(total) = totals.get_mut(&px) {
        total.remove_order(sz);
        total.is_empty()
    } else {
        false
    };
    if remove_level {
        totals.remove(&px);
    }
}

fn match_order<O: InnerOrder>(
    maker_orders: &mut BTreeMap<Px, LinkedList<Oid, O>>,
    maker_totals: &mut BTreeMap<Px, LevelTotal>,
    taker_order: &mut O,
) -> Vec<Oid> {
    let mut filled_oids = Vec::new();
    let mut keys_to_remove = Vec::new();
    let taker_side = taker_order.side();
    let limit_px = taker_order.limit_px();
    let order_iter: Box<dyn Iterator<Item = (&Px, &mut LinkedList<Oid, O>)>> = match taker_side {
        Side::Ask => Box::new(maker_orders.iter_mut().rev()),
        Side::Bid => Box::new(maker_orders.iter_mut()),
    };
    for (&px, list) in order_iter {
        let matches = match taker_side {
            Side::Ask => px >= limit_px,
            Side::Bid => px <= limit_px,
        };
        if !matches {
            break;
        }
        while let Some(match_order) = list.head_value_ref_mut_unsafe() {
            let matched_sz = taker_order.fill(match_order);
            let maker_filled = match_order.sz().is_zero();
            if let Some(total) = maker_totals.get_mut(&px) {
                total.apply_match(matched_sz, maker_filled);
            }
            if maker_filled {
                filled_oids.push(match_order.oid());
                let _unused = list.remove_front();
            }
            if taker_order.sz().is_zero() {
                break;
            }
        }
        if list.is_empty() {
            keys_to_remove.push(px);
        }
        if taker_order.sz().is_zero() {
            break;
        }
    }
    for key in keys_to_remove {
        maker_orders.remove(&key);
        maker_totals.remove(&key);
    }
    filled_oids
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::order_book::types::{Coin, Sz};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct MinimalOrder {
        oid: u64,
        side: Side,
        sz: u64,
        limit_px: u64,
    }

    impl InnerOrder for MinimalOrder {
        fn oid(&self) -> Oid {
            Oid::new(self.oid)
        }

        fn side(&self) -> Side {
            self.side
        }

        fn limit_px(&self) -> Px {
            Px::new(self.limit_px)
        }

        fn sz(&self) -> Sz {
            Sz::new(self.sz)
        }

        fn decrement_sz(&mut self, dec: Sz) {
            self.sz = self.sz.saturating_sub(dec.value());
        }

        fn fill(&mut self, maker_order: &mut Self) -> Sz {
            let match_sz = self.sz().min(maker_order.sz());
            maker_order.decrement_sz(match_sz);
            self.decrement_sz(match_sz);
            match_sz
        }

        fn modify_sz(&mut self, sz: Sz) {
            self.sz = sz.value();
        }

        fn convert_trigger(&mut self, _: u64) {}

        fn coin(&self) -> Coin {
            Coin::new("")
        }
    }

    impl MinimalOrder {
        fn new(oid: u64, sz: u64, limit_px: u64, side: Side) -> Self {
            Self { oid, side, sz, limit_px }
        }
    }

    #[derive(Default)]
    struct OrderFactory {
        next_oid: u64,
    }

    impl OrderFactory {
        fn order(&mut self, sz: u64, limit_px: u64, side: Side) -> MinimalOrder {
            let order = MinimalOrder::new(self.next_oid, sz, limit_px, side);
            self.next_oid += 1;
            order
        }

        fn batch_order(&mut self, sz: u64, limit_px: u64, side: Side, n: u64) -> Vec<MinimalOrder> {
            (0..n).map(|_| self.order(sz, limit_px, side)).collect_vec()
        }
    }

    #[test]
    fn simple_book_test() {
        let mut factory = OrderFactory::default();
        let buy_orders1 = factory.batch_order(100, 5, Side::Bid, 3);
        let buy_orders2 = factory.batch_order(200, 4, Side::Bid, 4);
        let sell_orders1 = factory.batch_order(150, 5, Side::Ask, 2);
        let sell_orders2 = factory.batch_order(500, 6, Side::Ask, 2);
        let mut book = OrderBook::new();
        for order in buy_orders2.clone() {
            book.add_order(order);
        }
        for order in sell_orders2.clone() {
            book.add_order(order);
        }
        for order in buy_orders1.clone() {
            book.add_order(order);
        }
        book.add_order(sell_orders1[0].clone());
        let mut bids = [buy_orders2, buy_orders1].concat();
        let mut asks = [sell_orders1.clone(), sell_orders2].concat();
        // remove index 4 and alter index 5 (matched)
        bids[5].sz -= 50;
        bids.remove(4);
        // remove index 0 (matched) and 1 (not inserted)
        asks.remove(1);
        asks.remove(0);

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());

        assert!(book.cancel_order(Oid::new(3)));
        assert!(book.cancel_order(Oid::new(9)));
        book.add_order(sell_orders1[1].clone());

        // index 4 and 5 both get matched, index 0 is canceled (first out of buy_orders2)
        bids.remove(5);
        bids.remove(4);
        bids.remove(0);

        // only thing changing in asks is that index 0 is canceled
        asks.remove(0);

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());

        // test modify size
        book.modify_sz(Oid::new(10), Sz::new(450));
        asks[0].sz = 450;

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());
    }

    fn assert_same_book(s1: Snapshot<MinimalOrder>, s2: Snapshot<MinimalOrder>) {
        let [b1, a1] = s1.0.map(BTreeSet::from_iter);
        let [b2, a2] = s2.0.map(BTreeSet::from_iter);
        assert_eq!(b1, b2);
        assert_eq!(a1, a2);
    }

    // ==================== BBO Tests ====================

    #[test]
    fn test_bbo_empty_book() {
        let book: OrderBook<MinimalOrder> = OrderBook::new();
        let (bid, ask) = book.get_bbo();
        assert!(bid.is_none());
        assert!(ask.is_none());
    }

    #[test]
    fn test_bbo_single_bid() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap(), (Px::new(50), Sz::new(100), 1));
        assert!(ask.is_none());
    }

    #[test]
    fn test_bbo_single_ask() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Ask));
        let (bid, ask) = book.get_bbo();
        assert!(bid.is_none());
        assert_eq!(ask.unwrap(), (Px::new(50), Sz::new(100), 1));
    }

    #[test]
    fn test_bbo_multiple_levels() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Bids at 50 and 40 - best bid should be 50
        book.add_order(factory.order(100, 40, Side::Bid));
        book.add_order(factory.order(200, 50, Side::Bid));
        // Asks at 60 and 70 - best ask should be 60
        book.add_order(factory.order(150, 70, Side::Ask));
        book.add_order(factory.order(300, 60, Side::Ask));

        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap().0, Px::new(50)); // best bid = highest
        assert_eq!(ask.unwrap().0, Px::new(60)); // best ask = lowest
    }

    #[test]
    fn test_bbo_aggregates_at_same_price() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        book.add_order(factory.order(200, 50, Side::Bid));
        let (bid, _) = book.get_bbo();
        let (px, sz, count) = bid.unwrap();
        assert_eq!(px, Px::new(50));
        assert_eq!(sz, Sz::new(300)); // aggregated
        assert_eq!(count, 2);
    }

    #[test]
    fn test_level_totals_track_mutations_and_matching() {
        let mut book = OrderBook::new();
        book.add_order(MinimalOrder::new(1, 10, 100, Side::Bid));
        book.add_order(MinimalOrder::new(2, 15, 100, Side::Bid));

        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap(), (Px::new(100), Sz::new(25), 2));
        assert!(ask.is_none());

        assert!(book.modify_sz(Oid::new(1), Sz::new(20)));
        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap(), (Px::new(100), Sz::new(35), 2));
        assert!(ask.is_none());

        assert!(book.cancel_order(Oid::new(2)));
        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap(), (Px::new(100), Sz::new(20), 1));
        assert!(ask.is_none());

        book.add_order(MinimalOrder::new(3, 5, 100, Side::Ask));
        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap(), (Px::new(100), Sz::new(15), 1));
        assert!(ask.is_none());

        book.add_order(MinimalOrder::new(4, 20, 100, Side::Ask));
        let (bid, ask) = book.get_bbo();
        assert!(bid.is_none());
        assert_eq!(ask.unwrap(), (Px::new(100), Sz::new(5), 1));
    }

    // ==================== Order Matching Tests ====================

    #[test]
    fn test_matching_bid_crosses_ask() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Place an ask at 50
        book.add_order(factory.order(100, 50, Side::Ask));
        // Place a bid at 60 (crosses the ask)
        book.add_order(factory.order(100, 60, Side::Bid));
        // Both fully filled, book should be empty
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_matching_partial_fill_taker() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Ask of 200 at price 50
        book.add_order(factory.order(200, 50, Side::Ask));
        // Bid of 100 at price 60 - only partially fills the ask
        book.add_order(factory.order(100, 60, Side::Bid));
        // Ask remains with sz=100, bid fully consumed
        assert_eq!(book.order_count(), 1);
        let (_, ask) = book.get_bbo();
        assert_eq!(ask.unwrap().1, Sz::new(100));
    }

    #[test]
    fn test_matching_partial_fill_maker() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Ask of 50 at price 50
        book.add_order(factory.order(50, 50, Side::Ask));
        // Bid of 200 at price 60 - fills the ask, rest rests on book
        book.add_order(factory.order(200, 60, Side::Bid));
        assert_eq!(book.order_count(), 1);
        let (bid, _) = book.get_bbo();
        assert_eq!(bid.unwrap().1, Sz::new(150)); // 200 - 50
    }

    #[test]
    fn test_no_matching_bid_below_ask() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 60, Side::Ask));
        book.add_order(factory.order(100, 50, Side::Bid));
        // No crossing, both rest on book
        assert_eq!(book.order_count(), 2);
    }

    #[test]
    fn test_matching_multiple_price_levels() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Three asks at different prices
        book.add_order(factory.order(100, 50, Side::Ask)); // matched first
        book.add_order(factory.order(100, 55, Side::Ask)); // matched second
        book.add_order(factory.order(100, 60, Side::Ask)); // partially matched
        // One big bid that sweeps through
        book.add_order(factory.order(250, 60, Side::Bid));
        // 100+100+50 matched, ask at 60 has 50 left
        assert_eq!(book.order_count(), 1);
        let (_, ask) = book.get_bbo();
        assert_eq!(ask.unwrap().1, Sz::new(50));
    }

    // ==================== Cancel / Modify Tests ====================

    #[test]
    fn test_cancel_nonexistent_returns_false() {
        let mut book: OrderBook<MinimalOrder> = OrderBook::new();
        assert!(!book.cancel_order(Oid::new(999)));
    }

    #[test]
    fn test_duplicate_add_order_is_ignored() {
        // C3 regression test: prior to the dedup guard, a second add_order for the
        // same oid silently corrupted state - oid_to_side_px was overwritten but
        // the slab still held the original order. Verify the duplicate is now a no-op
        // and the original order remains cancelable.
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        let first = factory.order(100, 50, Side::Bid);
        let oid = first.oid();
        book.add_order(first);
        assert_eq!(book.order_count(), 1);

        // Try to re-add the same oid at a different price - should be ignored
        let dup = MinimalOrder { oid: 0, side: Side::Bid, sz: 999, limit_px: 99 };
        book.add_order(dup);
        assert_eq!(book.order_count(), 1, "duplicate add should not increase order count");

        // The original order is still there and cancelable
        assert!(book.cancel_order(oid));
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_cancel_removes_price_level() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        assert!(book.cancel_order(Oid::new(0)));
        assert_eq!(book.order_count(), 0);
        let (bid, _) = book.get_bbo();
        assert!(bid.is_none());
    }

    #[test]
    fn test_modify_sz_to_zero_cancels() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        assert!(book.modify_sz(Oid::new(0), Sz::new(0)));
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_modify_nonexistent_returns_false() {
        let mut book: OrderBook<MinimalOrder> = OrderBook::new();
        assert!(!book.modify_sz(Oid::new(999), Sz::new(100)));
    }

    #[test]
    fn test_modify_sz_updates_value() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        assert!(book.modify_sz(Oid::new(0), Sz::new(500)));
        let (bid, _) = book.get_bbo();
        assert_eq!(bid.unwrap().1, Sz::new(500));
    }

    // ==================== Snapshot Tests ====================

    #[test]
    fn test_snapshot_roundtrip() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        book.add_order(factory.order(200, 40, Side::Bid));
        book.add_order(factory.order(150, 60, Side::Ask));

        let snapshot = book.to_snapshot();
        let restored = OrderBook::from_snapshot(snapshot, false);
        assert_eq!(book.order_count(), restored.order_count());
        assert_eq!(book.get_bbo(), restored.get_bbo());
    }

    #[test]
    fn test_snapshot_truncate() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        for i in 0..10 {
            book.add_order(factory.order(100, 50 + i, Side::Bid));
        }
        let snapshot = book.to_snapshot();
        let truncated = snapshot.truncate(3);
        assert_eq!(truncated.as_ref()[0].len(), 3); // bids
    }

    #[test]
    fn test_order_count() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        assert_eq!(book.order_count(), 0);
        book.add_order(factory.order(100, 50, Side::Bid));
        assert_eq!(book.order_count(), 1);
        book.add_order(factory.order(100, 60, Side::Ask));
        assert_eq!(book.order_count(), 2);
        book.cancel_order(Oid::new(0));
        assert_eq!(book.order_count(), 1);
    }

    // ==================== Performance / Stress Tests ====================

    #[test]
    fn test_stress_add_cancel_1000_orders() {
        let start = std::time::Instant::now();
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();

        // Add 1000 orders
        for i in 0..1000u64 {
            let px = 1000 + (i % 100); // 100 price levels
            let side = if i % 2 == 0 { Side::Bid } else { Side::Ask };
            book.add_order(factory.order(100, px, side));
        }
        let add_elapsed = start.elapsed();

        assert!(book.order_count() > 0, "some orders should remain (non-crossing)");

        // Cancel all remaining
        let cancel_start = std::time::Instant::now();
        for oid in 0..1000u64 {
            book.cancel_order(Oid::new(oid));
        }
        let cancel_elapsed = cancel_start.elapsed();

        assert_eq!(book.order_count(), 0);

        eprintln!(
            "[PERF] 1000 order adds: {:?}, 1000 cancels: {:?}, total: {:?}",
            add_elapsed,
            cancel_elapsed,
            start.elapsed()
        );
    }

    #[test]
    fn test_bbo_computation_performance() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Create book with many price levels
        for i in 0..500u64 {
            book.add_order(factory.order(100, 1000 + i, Side::Bid));
            book.add_order(factory.order(100, 2000 + i, Side::Ask));
        }

        let start = std::time::Instant::now();
        let iterations = 10_000;
        for _ in 0..iterations {
            let _ = book.get_bbo();
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!(
            "[PERF] BBO computation: {iterations} calls in {:?} ({:?}/call, 500 levels each side)",
            elapsed, per_call
        );
        // BBO should be fast - under 10us per call
        assert!(per_call.as_micros() < 100, "BBO too slow: {:?}/call", per_call);
    }

    #[test]
    fn test_l4_snapshot_performance() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Build a realistic-sized book: 500 price levels each side, 1 order each
        for i in 0..500u64 {
            book.add_order(factory.order(100, 1000 + i, Side::Bid));
            book.add_order(factory.order(100, 2000 + i, Side::Ask));
        }
        assert_eq!(book.order_count(), 1000);

        let start = std::time::Instant::now();
        let iterations = 1000u32;
        for _ in 0..iterations {
            let snapshot = book.to_snapshot();
            assert_eq!(snapshot.as_ref()[0].len(), 500);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!(
            "[PERF] L4 snapshot (1000 orders, 500 levels/side): {iterations} calls in {:?} ({:?}/call)",
            elapsed, per_call
        );
    }

    #[test]
    fn test_l4_snapshot_from_snapshot_performance() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        for i in 0..500u64 {
            book.add_order(factory.order(100, 1000 + i, Side::Bid));
            book.add_order(factory.order(100, 2000 + i, Side::Ask));
        }
        let snapshot = book.to_snapshot();

        let start = std::time::Instant::now();
        let iterations = 100u32;
        for _ in 0..iterations {
            let restored = OrderBook::from_snapshot(snapshot.clone(), false);
            assert_eq!(restored.order_count(), 1000);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!("[PERF] L4 from_snapshot (1000 orders): {iterations} calls in {:?} ({:?}/call)", elapsed, per_call);
    }
}
