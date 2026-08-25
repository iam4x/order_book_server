use std::{hash::Hash, marker::PhantomData};

use rustc_hash::FxHashMap;
use slab::Slab;

use crate::prelude::*;

#[derive(Clone)]
struct Node<K, T> {
    key: K,
    value: T,
    next: Option<usize>,
    prev: Option<usize>,
}

impl<K, T> Node<K, T> {
    pub(crate) const fn new(key: K, value: T) -> Self {
        Self { key, value, next: None, prev: None }
    }
}

#[derive(Clone)]
// Implicit assumption is that when we remove a node, it is never used again
pub(crate) struct LinkedList<K, T> {
    key_to_sid: FxHashMap<K, usize>,
    slab: Slab<Node<K, T>>,
    head: Option<usize>,
    tail: Option<usize>,
    phantom_data: PhantomData<T>,
}

impl<K: Clone + Eq + Hash, T: Clone> LinkedList<K, T> {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { key_to_sid: FxHashMap::default(), slab: Slab::new(), head: None, tail: None, phantom_data: PhantomData }
    }

    pub(crate) fn push_back(&mut self, key: K, value: T) -> bool {
        if self.key_to_sid.contains_key(&key) {
            false
        } else {
            let node = Node::new(key.clone(), value);
            let sid = self.slab.insert(node);
            self.key_to_sid.insert(key, sid);
            match self.tail {
                None => {
                    self.head = Some(sid);
                    self.tail = Some(sid);
                }
                Some(t) => {
                    let tail_order = &mut self.slab[t];
                    tail_order.next = Some(sid);
                    let new_order = &mut self.slab[sid];
                    new_order.prev = Some(t);
                    self.tail = Some(sid);
                }
            }
            true
        }
    }

    pub(crate) fn insert_before(&mut self, before: &K, key: K, value: T) -> bool {
        if self.key_to_sid.contains_key(&key) {
            return false;
        }
        let Some(&before_sid) = self.key_to_sid.get(before) else {
            return false;
        };

        let prev = self.slab[before_sid].prev;
        let mut node = Node::new(key.clone(), value);
        node.prev = prev;
        node.next = Some(before_sid);

        let sid = self.slab.insert(node);
        self.key_to_sid.insert(key, sid);
        self.slab[before_sid].prev = Some(sid);

        if let Some(prev_sid) = prev {
            self.slab[prev_sid].next = Some(sid);
        } else {
            self.head = Some(sid);
        }

        true
    }

    #[must_use]
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.key_to_sid.contains_key(key)
    }

    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    pub(crate) fn head_value_ref_mut_unsafe(&mut self) -> Option<&mut T> {
        self.head.as_ref().map(|&h| &mut self.slab[h].value)
    }

    pub(crate) fn remove_front(&mut self) -> Result<()> {
        if let Some(h) = self.head {
            let new_head = {
                let head_order = self.slab.remove(h);
                self.key_to_sid.remove(&head_order.key);
                head_order.next
            };
            match new_head {
                None => {
                    self.head = None;
                    self.tail = None;
                }
                Some(n) => {
                    self.head = Some(n);
                    let new_order = &mut self.slab[n];
                    new_order.prev = None;
                }
            }
            Ok(())
        } else {
            Err("List is empty".into())
        }
    }

    pub(crate) fn remove_node(&mut self, key: K) -> bool {
        if let Some((_, sid)) = self.key_to_sid.remove_entry(&key) {
            let (prev, next) = {
                let order = self.slab.remove(sid);
                (order.prev, order.next)
            };
            if let Some(p) = prev {
                let prev_order = &mut self.slab[p];
                prev_order.next = next;
            } else {
                self.head = next;
            }
            if let Some(n) = next {
                let next_order = &mut self.slab[n];
                next_order.prev = prev;
            } else {
                self.tail = prev;
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn node_value_mut(&mut self, key: &K) -> Option<&mut T> {
        if let Some(sid) = self.key_to_sid.get(key) { Some(&mut self.slab[*sid].value) } else { None }
    }

    pub(crate) fn node_value(&self, key: &K) -> Option<&T> {
        self.key_to_sid.get(key).map(|sid| &self.slab[*sid].value)
    }

    #[must_use]
    pub(crate) fn to_vec(&self) -> Vec<&T> {
        let mut res = Vec::new();
        let mut cur = self.head;
        while let Some(c) = cur {
            let node = &self.slab[c];
            res.push(&node.value);
            cur = node.next;
        }
        res
    }

    #[allow(dead_code)]
    pub(crate) fn fold<F, Acc>(&self, mut init: Acc, f: F) -> Acc
    where
        F: Fn(&mut Acc, &T),
    {
        let mut cur = self.head;
        while let Some(c) = cur {
            let node = &self.slab[c];
            f(&mut init, &node.value);
            cur = node.next;
        }
        init
    }

    /// Number of live nodes in the slab.
    pub(crate) fn slab_len(&self) -> usize {
        self.slab.len()
    }

    /// Allocated slab capacity, including free slots from removed nodes.
    /// `Slab::remove` marks a slot free but never releases the underlying `Vec`,
    /// so after many add/cancel cycles this grows to the high-water mark of
    /// concurrent orders. Compaction is the only way to reclaim it.
    pub(crate) fn slab_capacity(&self) -> usize {
        self.slab.capacity()
    }

    /// Fraction of slab capacity that is currently unused. Returns 0.0 when
    /// capacity is 0 to avoid division by zero.
    #[allow(dead_code)]
    pub(crate) fn fragmentation_ratio(&self) -> f64 {
        let cap = self.slab.capacity();
        if cap == 0 {
            return 0.0;
        }
        1.0 - (self.slab.len() as f64 / cap as f64)
    }

    /// Rebuild the slab from scratch when it is heavily over-allocated, releasing
    /// the slots that `slab::Slab::remove` left behind. No-op below the threshold
    /// so this is safe to call on every maintenance tick.
    pub(crate) fn compact(&mut self) -> bool {
        if self.slab.capacity() <= 2 * self.slab.len() + 64 {
            return false;
        }
        let live = self.slab.len();
        let mut items: Vec<(K, T)> = Vec::with_capacity(live);
        let mut cur = self.head;
        while let Some(c) = cur {
            let node = &self.slab[c];
            items.push((node.key.clone(), node.value.clone()));
            cur = node.next;
        }
        self.slab = Slab::with_capacity(live);
        self.key_to_sid = FxHashMap::with_capacity_and_hasher(live, rustc_hash::FxBuildHasher);
        self.head = None;
        self.tail = None;
        for (key, value) in items {
            self.push_back(key, value);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use itertools::Itertools;

    use super::*;

    #[must_use]
    fn to_rev_vec<K: Clone + Eq + Hash, T: Clone>(list: &LinkedList<K, T>) -> Vec<&T> {
        let mut res = Vec::new();
        let mut cur = list.tail;
        while let Some(c) = cur {
            let node = &list.slab[c];
            res.push(&node.value);
            cur = node.prev;
        }
        res
    }

    #[test]
    fn simple_linked_list_test() -> Result<()> {
        let mut deque = (0..11).collect::<VecDeque<_>>();
        let mut keys = Vec::new();
        let mut list = LinkedList::new();
        for &elt in &deque {
            keys.push(elt);
            list.push_back(elt, elt);
        }

        assert_vec_deque_list_eq(&deque, &list);

        list.remove_front()?;
        deque.pop_front();

        assert_vec_deque_list_eq(&deque, &list);

        list.remove_front()?;
        deque.pop_front();

        assert_vec_deque_list_eq(&deque, &list);

        list.remove_node(keys[4]);
        deque.remove(2);

        assert_vec_deque_list_eq(&deque, &list);

        for _ in 0..5 {
            list.remove_front()?;
            deque.pop_front();
            assert_vec_deque_list_eq(&deque, &list);
        }

        for k in keys.iter().skip(8) {
            list.remove_node(*k);
            deque.pop_front();
            assert_vec_deque_list_eq(&deque, &list);
        }

        assert!(list.is_empty());
        Ok(())
    }

    fn assert_vec_deque_list_eq<K: Clone + Eq + Hash, T: Debug + Clone + Eq + PartialEq>(
        deque: &VecDeque<T>,
        list: &LinkedList<K, T>,
    ) {
        let evec = deque.iter().cloned().collect_vec();
        for (a, b) in list.to_vec().iter().zip(evec.iter()) {
            assert_eq!(**a, *b);
        }
        let mut rev = evec;
        rev.reverse();
        for (a, b) in to_rev_vec(list).iter().zip(rev.iter()) {
            assert_eq!(**a, *b);
        }
    }

    #[test]
    fn test_push_back_duplicate_key_returns_false() {
        let mut list = LinkedList::new();
        assert!(list.push_back(1, "a"));
        assert!(!list.push_back(1, "b")); // duplicate key
        assert_eq!(list.to_vec(), vec![&"a"]); // value unchanged
    }

    #[test]
    fn test_insert_before_head() {
        let mut list = LinkedList::new();
        assert!(list.push_back(1, "a"));
        assert!(list.push_back(2, "b"));

        assert!(list.insert_before(&1, 3, "c"));

        assert_eq!(list.to_vec(), vec![&"c", &"a", &"b"]);
        assert_eq!(to_rev_vec(&list), vec![&"b", &"a", &"c"]);
    }

    #[test]
    fn test_insert_before_middle() {
        let mut list = LinkedList::new();
        assert!(list.push_back(1, "a"));
        assert!(list.push_back(2, "b"));
        assert!(list.push_back(3, "c"));

        assert!(list.insert_before(&3, 4, "d"));

        assert_eq!(list.to_vec(), vec![&"a", &"b", &"d", &"c"]);
        assert_eq!(to_rev_vec(&list), vec![&"c", &"d", &"b", &"a"]);
    }

    #[test]
    fn test_insert_before_missing_anchor_returns_false() {
        let mut list = LinkedList::new();
        assert!(list.push_back(1, "a"));

        assert!(!list.insert_before(&9, 2, "b"));

        assert_eq!(list.to_vec(), vec![&"a"]);
    }

    #[test]
    fn test_insert_before_duplicate_key_returns_false() {
        let mut list = LinkedList::new();
        assert!(list.push_back(1, "a"));
        assert!(list.push_back(2, "b"));

        assert!(!list.insert_before(&2, 1, "c"));

        assert_eq!(list.to_vec(), vec![&"a", &"b"]);
    }

    #[test]
    fn test_remove_front_empty_list_returns_err() {
        let mut list: LinkedList<i32, i32> = LinkedList::new();
        assert!(list.remove_front().is_err());
    }

    #[test]
    fn test_remove_node_nonexistent_returns_false() {
        let mut list = LinkedList::new();
        list.push_back(1, "a");
        assert!(!list.remove_node(999));
    }

    #[test]
    fn test_remove_node_head() {
        let mut list = LinkedList::new();
        list.push_back(1, "a");
        list.push_back(2, "b");
        list.push_back(3, "c");
        assert!(list.remove_node(1)); // remove head
        assert_eq!(list.to_vec(), vec![&"b", &"c"]);
        assert_eq!(to_rev_vec(&list), vec![&"c", &"b"]);
    }

    #[test]
    fn test_remove_node_tail() {
        let mut list = LinkedList::new();
        list.push_back(1, "a");
        list.push_back(2, "b");
        list.push_back(3, "c");
        assert!(list.remove_node(3)); // remove tail
        assert_eq!(list.to_vec(), vec![&"a", &"b"]);
        assert_eq!(to_rev_vec(&list), vec![&"b", &"a"]);
    }

    #[test]
    fn test_remove_node_middle() {
        let mut list = LinkedList::new();
        list.push_back(1, "a");
        list.push_back(2, "b");
        list.push_back(3, "c");
        assert!(list.remove_node(2)); // remove middle
        assert_eq!(list.to_vec(), vec![&"a", &"c"]);
        assert_eq!(to_rev_vec(&list), vec![&"c", &"a"]);
    }

    #[test]
    fn test_remove_only_element() {
        let mut list = LinkedList::new();
        list.push_back(1, "a");
        assert!(list.remove_node(1));
        assert!(list.is_empty());
        assert!(list.to_vec().is_empty());
    }

    #[test]
    fn test_node_value_mut() {
        let mut list = LinkedList::new();
        list.push_back(1, 10);
        list.push_back(2, 20);
        if let Some(v) = list.node_value_mut(&1) {
            *v = 99;
        }
        assert_eq!(list.to_vec(), vec![&99, &20]);
    }

    #[test]
    fn test_node_value_mut_nonexistent() {
        let mut list: LinkedList<i32, i32> = LinkedList::new();
        assert!(list.node_value_mut(&1).is_none());
    }

    #[test]
    fn test_head_value_ref_mut_unsafe() {
        let mut list = LinkedList::new();
        assert!(list.head_value_ref_mut_unsafe().is_none());
        list.push_back(1, 42);
        assert_eq!(list.head_value_ref_mut_unsafe(), Some(&mut 42));
    }

    #[test]
    fn test_fold() {
        let mut list = LinkedList::new();
        list.push_back(1, 10);
        list.push_back(2, 20);
        list.push_back(3, 30);
        let sum = list.fold(0, |acc, val| *acc += val);
        assert_eq!(sum, 60);
    }

    #[test]
    fn test_fold_empty() {
        let list: LinkedList<i32, i32> = LinkedList::new();
        let sum = list.fold(0, |acc, val| *acc += val);
        assert_eq!(sum, 0);
    }

    #[test]
    fn test_is_empty() {
        let mut list = LinkedList::new();
        assert!(list.is_empty());
        list.push_back(1, "a");
        assert!(!list.is_empty());
        list.remove_front().unwrap();
        assert!(list.is_empty());
    }

    #[test]
    fn test_large_list_operations() {
        let mut list = LinkedList::new();
        for i in 0..1000 {
            assert!(list.push_back(i, i * 10));
        }
        assert_eq!(list.to_vec().len(), 1000);
        // Remove every other element
        for i in (0..1000).step_by(2) {
            assert!(list.remove_node(i));
        }
        assert_eq!(list.to_vec().len(), 500);
    }

    #[test]
    fn test_compact_reclaims_capacity_after_churn() {
        let mut list = LinkedList::new();
        // Build up a high-water mark of 10_000 live nodes
        for i in 0..10_000u32 {
            list.push_back(i, i);
        }
        // Remove 9_900 of them; slab capacity stays at high-water mark
        for i in 0..9_900u32 {
            list.remove_node(i);
        }
        let cap_before = list.slab_capacity();
        let len_before = list.slab_len();
        assert_eq!(len_before, 100);
        assert!(cap_before > 2 * len_before + 64, "slab should be heavily fragmented before compact");

        assert!(list.compact());

        let cap_after = list.slab_capacity();
        let len_after = list.slab_len();
        assert_eq!(len_after, 100); // live nodes preserved
        assert!(cap_after < cap_before, "compact should shrink capacity: {cap_before} -> {cap_after}");

        // Order preserved
        let values: Vec<u32> = list.to_vec().into_iter().copied().collect();
        let expected: Vec<u32> = (9_900..10_000).collect();
        assert_eq!(values, expected);

        // No-op when nothing to compact
        assert!(!list.compact());
    }
}
