#[macro_use]
mod node;
mod key;
mod value;
mod partial_key;
mod node_impl;
// mod insert;

use crate::epoch::{self, Atomic, Owned, Shared, Inline, Guard};
use node::{NodeTemplate, NodeKind};
use node_impl::SP8NodeMut;
use std::sync::atomic::Ordering;
use std::ops::{Deref, DerefMut};
use self::node::{NodeOps, NodeSyncOps, NodeReadDataOps, NodeWriteDataOps, NodeSearchOps};
use self::key::{Key, ExtractKey, ExtractTID};
use self::value::ValueLoader;

pub struct HOT {
    root: Atomic<NodeTemplate>,
}

impl HOT {
    /// Create an empty HOT.
    #[inline]
    pub fn new() -> Self {
        HOT{
            root: Atomic::from(NodeTemplate::empty()),
        }
    }

    #[inline]
    pub fn lookup<'k, L: ValueLoader>(&self, key: Key<'k>, loader: &L, guard: &epoch::Guard) -> Option<L::Value> {
        let mut node = self.root.load(Ordering::Acquire, guard);
        loop {
            match node.kind() {
                NodeKind::Empty => return None, // trie is empty.
                NodeKind::Leaf => {
                    let value = loader.load_leaf(node);
                    let k = value.extract_key();
                    return if &*k == &*key {
                        Some(value)
                    } else {
                        // key does not match
                        None
                    }
                }
                _ => {
                    if let Some(res) = try_opt_read_node!(node, |_, n| {
                        n.search_partial_key(&key).map(|idx| n.value(idx).load(Ordering::Acquire, guard))
                    }) {
                        if let Some(new) = res {
                            node = new;
                        } else {
                            return None
                        }
                    } else {
                        // optimistic read failed: retry from root.
                        self.root.load(Ordering::Acquire, guard);
                    }
                }
            }
        }
    }

    #[inline]
    pub fn insert<V: ExtractKey + ExtractTID, L: ValueLoader<Value=V>>(&self, value: V, loader: &L, guard: &epoch::Guard) -> bool {
        let new_key = &*value.extract_key();
        let new_tid = value.extract_tid();
        loop {
            match self.insert_internal(new_key, new_tid, loader, guard) {
                InsertResult::Stalled => (),
                InsertResult::Ok => return true,
                InsertResult::Duplicated => return false,
            }
        }
    }

    #[inline]
    fn insert_internal<L: ValueLoader>(&self, new_key: &[u8], new_tid: u64, loader: &L, guard: &epoch::Guard) -> InsertResult {
        let root = &self.root;
        let node = root.load(Ordering::Acquire, guard);
        match node.kind() {
            NodeKind::Empty => {
                // empty tree, just replace
                let new = NodeTemplate::tid(new_tid);
                match root.compare_exchange_weak(node, new, Ordering::SeqCst, Ordering::Relaxed, guard) {
                    Ok(_) => InsertResult::Ok,
                    Err(_) => InsertResult::Stalled,
                }
            }
            NodeKind::Leaf => {
                // single value
                let new = match BiNodeBuilder::from_leaf_and_new_key(node, new_key, 0, new_tid, loader) {
                    Some(bn) => bn.build(),
                    None => return InsertResult::Duplicated,
                };
                match root.compare_exchange_weak(node, new, Ordering::SeqCst, Ordering::Relaxed, guard) {
                    Ok(_) => InsertResult::Ok,
                    Err(_) => InsertResult::Stalled,
                }
            }
            _ => {
                let mut stack = InsertStack::new(); // todo: thread local cache
                self.insert_with_stack(node, new_key, new_tid, loader, &mut stack, guard)
            }
        }
    }

    #[inline]
    fn insert_with_stack<'g, L: ValueLoader>(&self, node: Shared<'g, NodeTemplate>, new_key: &[u8], new_tid: u64, loader: &L, stack: &mut InsertStack<'g>, guard: &'g epoch::Guard) -> InsertResult {
        self.search_for_insert(new_key, stack, guard);
        let depth = stack.len();
        let entry = &stack[depth-1];
        debug_assert!(entry.node.kind() == NodeKind::Leaf);
        // depth always greater than 0, because root leaf is examined before this method
        let parent = &stack[depth-2];
        let height = parent.node.height();
        if height > 1 {
            // perform leaf push down because it won't increase tree height.
            let new = match BiNodeBuilder::from_leaf_and_new_key(node, new_key, parent.msb_absolute_idx, new_tid, loader) {
                Some(bn) => bn.build(),
                None => return InsertResult::Duplicated,
            };
            let node = parent.node;
            with_write_lock_node!(node, |mut n| {
                // it's ok to overwrite the original value.
                n.value_mut(parent.index as usize).store(new, Ordering::Relaxed);
            });
            return InsertResult::Ok;
        }
        // todo: normal insert, parent pull up, or create intermediate node.
        todo!()
    }

    #[inline]
    fn search_for_insert<'g>(&self, key: &[u8], stack: &mut InsertStack<'g>, guard: &'g Guard) {
        let mut curr = self.root.load(Ordering::Acquire, guard);
        loop {
            match curr.kind() {
                NodeKind::Empty => unreachable!(),
                NodeKind::Leaf => break,
                _ => {
                    if let Some((new, entry)) = try_opt_read_node!(curr, |version, n| {
                        let index = n.search_partial_key(key).unwrap();
                        let entry = InsertStackEntry{
                            version,
                            node: curr,
                            index: index as u8,
                            msb_absolute_idx: n.msb_offset(),
                        };
                        let shared = n.value(index).load(Ordering::Acquire, guard);
                        (shared, entry)
                    }) {
                        curr = new;
                        stack.push(entry);
                    } else {
                        // retry from root
                        curr = self.root.load(Ordering::Acquire, guard);
                        stack.clear();
                    }
                }
            }
        }
        stack.push(InsertStackEntry{
            version: 0, // leaf does not have version.
            node: curr,
            index: 0,
            msb_absolute_idx: !0,
        });
    }
}

// const U8_WITH_MOST_SIGNIFICANT_BIT: u8 = 0b1000_0000;
const U64_WITH_MOST_SIGNIFICANT_BIT: u64 = 0x8000_0000_0000_0000;

enum InsertResult {
    Ok,
    Stalled,
    Duplicated,
}

struct BiNodeBuilder<'g> {
    height: u8,
    msb_absolute_idx: u16,
    curr_byte: u8,
    new_byte: u8,
    curr_node: Shared<'g, NodeTemplate>,
    new_node: Inline<NodeTemplate>,
}

impl<'g> BiNodeBuilder<'g> {

    /// Create a new node by given leaf and new key.
    /// The returned node contains only two values, so called BiNode.
    #[inline]
    fn from_leaf_and_new_key<L: ValueLoader>(leaf: Shared<'g, NodeTemplate>, new_key: &[u8], msb_absolute_idx: u16, new_tid: u64, loader: &L) -> Option<Self> {
        let curr = loader.load_leaf(leaf);
        let curr_key = &*curr.extract_key();
        let start_byte_idx = msb_absolute_idx as usize / 8;

        // todo: two keys may have different length,
        // we always extend the shorter key with sequence of 0x00 at the end.
        // There are some consequences:
        // 1. We cannot identify keys followed by different number of 0x00.
        // 2. key may have not enough bytes, according to msb_absolute_idx, we 
        //    also have to extend it.
        for (i, (nb, cb)) in new_key[start_byte_idx..].iter().zip(&curr_key[start_byte_idx..]).enumerate() {
            if nb != cb {
                let new_msb_absolute_idx = (start_byte_idx+i) * 8 + (nb ^ cb).leading_zeros() as usize;
                let bi_node = BiNodeBuilder{
                    height: 1, 
                    msb_absolute_idx: new_msb_absolute_idx as u16, 
                    curr_byte: *cb, 
                    new_byte: *nb, 
                    curr_node: leaf, 
                    new_node: NodeTemplate::tid(new_tid),
                };
                return Some(bi_node)
            }
        }
        None
    }

    #[inline]
    fn build(self) -> Owned<NodeTemplate> {
        unsafe {
            let mut node = NodeTemplate::new(self.height, 2, NodeKind::SP8);
            let mut sp8node = SP8NodeMut::from(&mut node);
            // initialize offset and single mask.
            sp8node.set_msb_offset(self.msb_absolute_idx);
            sp8node.set_mask(self.bit_mask());
            // initialize partial keys
            // for two-entry node, the first partial key must be 0,
            // and second must be 1.
            sp8node.set_partial_key(0, 0);
            sp8node.set_partial_key(1, 1);
            // initialize values
            sp8node.value_mut_unchecked(0).write(Atomic::from(self.curr_node.into_inline()));
            sp8node.value_mut_unchecked(1).write(Atomic::from(self.new_node));
            node
        }
    }

    #[inline]
    fn bit_mask(&self) -> u64 {
        U64_WITH_MOST_SIGNIFICANT_BIT >> (self.curr_byte ^ self.new_byte).leading_zeros()
    }
}

pub struct InsertStack<'g> {
    inner: Vec<InsertStackEntry<'g>>,
}

impl<'g> Deref for InsertStack<'g> {
    type Target = Vec<InsertStackEntry<'g>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'g> DerefMut for InsertStack<'g> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'g> InsertStack<'g> {
    #[inline]
    fn new() -> Self {
        InsertStack{inner: Vec::new()}
    }
}

impl Drop for HOT {
    fn drop(&mut self) {
        unsafe {
            let guard = epoch::unprotected();
            drop_subtree(&self.root, guard);
        }
    }
}

unsafe fn drop_subtree(root: &Atomic<NodeTemplate>, guard: &Guard) {
    let node = root.load(Ordering::Relaxed, guard);
    match node.kind() {
        NodeKind::Empty | NodeKind::Leaf => (),
        _ => {
            unchecked_read_node!(node, |n| {
                for i in 0..n.n_values() {
                    let c = n.value(i);
                    drop_subtree(c, guard);
                }
            });
        }
    }
    guard.defer_destroy(node);
}


#[derive(Clone, Copy)]
pub struct InsertStackEntry<'g> {
    // version of current node.
    version: u64,
    // node along the insert path.
    // This is the shared copy of original node pointer,
    // we if we want to change its content, we cannot perform CAS.
    // we have to find its parent, perform write lock, and update its
    // value at specific position.
    node: Shared<'g, NodeTemplate>,
    // index of the return entry.
    index: u8,
    // absolute position of most significant bit.
    // discriminative byte index.
    msb_absolute_idx: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot::node::{NodeSyncOps, NodeReadDataOps};
    use crate::hot::value::EmbeddedU32;

    #[test]
    fn test_leading_zeros() {
        for (a, b) in [
            (1u8, 2u8),
            (128, 0),
            (10, 10),
        ] {
            let v = a ^ b;
            println!("a={}, b={}, a^b={}, clz={}", a, b, v, v.leading_zeros());
        }
    }

    #[test]
    fn test_hot_insert_ops() {
        let hot = HOT::new();
        let guard = unsafe { epoch::unprotected() };
        let node = hot.root.load(Ordering::Acquire, &guard);
        assert_eq!(NodeKind::Empty, node.kind());
        // test insert empty
        hot.insert(1u32, &EmbeddedU32, &guard);
        let node2 = hot.root.load(Ordering::Acquire, &guard);
        assert_eq!(NodeKind::Empty, node.kind());
        assert_eq!(NodeKind::Leaf, node2.kind());
        // test insert leaf
        hot.insert(2u32, &EmbeddedU32, &guard);
        let node3 = hot.root.load(Ordering::Acquire, &guard);
        assert_eq!(NodeKind::SP8, node3.kind());

        try_opt_read_node!(node3, |_, n| {
            assert_eq!(0, n.partial_key(0));
            assert_eq!(1, n.partial_key(1));
        });
        assert_eq!(Some(1), hot.lookup(1u32.extract_key(), &EmbeddedU32, guard));
        assert_eq!(Some(2), hot.lookup(2u32.extract_key(), &EmbeddedU32, guard));
        assert_eq!(None, hot.lookup(4u32.extract_key(), &EmbeddedU32, guard));
        assert_eq!(None, hot.lookup(5u32.extract_key(), &EmbeddedU32, guard));
        // test insert sp8
        // hot.insert(3u32, &EmbeddedU32, &guard);
    }
}