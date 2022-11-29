use super::node_impl::{SP16Node, SP32Node, SP8NodeRef};
use crate::epoch::{self, low_bits, Atomic, Inline, Owned, Pointable, Shared};
// use super::partial_key::NodePartialKeyOps;
// use super::value::NodeValueOps;
use parking_lot::lock_api::RawRwLock as RawRwLockAPI;
use parking_lot::RawRwLock;
use scopeguard::defer;
use std::alloc;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{self, MaybeUninit};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Empty = 0,
    Leaf = 1,
    SP8 = 2,
    SP16 = 3,
    SP32 = 4,
    MP8 = 5,
    MP16 = 6,
    MP32 = 7,
}

impl NodeKind {
    #[inline]
    pub fn mask_size(self) -> usize {
        match self {
            NodeKind::Empty | NodeKind::Leaf => 0,
            NodeKind::SP8 | NodeKind::SP16 | NodeKind::SP32 => mem::size_of::<u64>(), // u16 offset(embedded in header) + u64 mask
            NodeKind::MP8 => 8 * mem::size_of::<u16>() + 8 * mem::size_of::<u8>(), // 8 * u16 offset + 8 * u8 mask
            NodeKind::MP16 => 16 * mem::size_of::<u16>() + 16 * mem::size_of::<u16>(), // 16 * u16 offset + 16 * u16 mask
            NodeKind::MP32 => 32 * mem::size_of::<u16>() + 32 * mem::size_of::<u32>(), // 32 * u16 offset + 32 * u32 mask
        }
    }
}

impl From<u8> for NodeKind {
    #[inline]
    fn from(src: u8) -> Self {
        match src {
            0 => NodeKind::Empty,
            1 => NodeKind::Leaf,
            2 => NodeKind::SP8,
            3 => NodeKind::SP16,
            4 => NodeKind::SP32,
            5 => NodeKind::MP8,
            6 => NodeKind::MP16,
            7 => NodeKind::MP32,
            v => panic!("invalid node kind {}", v),
        }
    }
}

/// Node with header and data.
/// The actual data is dynamic sized and allocated via construction.
/// Convert [`Node`] to [`super::node_impl::NodeImpl`] to gain full functionality.
/// align is set to 8 bytes, because we use 3 lowest bits as node type,
/// and allow plain value shifted to store in the pointer.
#[repr(C, align(8))]
pub struct NodeTemplate {
    /* first word */
    // version to support optimistic read.
    pub(super) version: AtomicU64,

    /* second word */
    // lock for write access.
    pub(super) lock: RawRwLock,

    /* third word */
    // 65535 bytes is big enough for all types of nodes.
    pub(super) data_len: u16,
    // height of current node.
    pub(super) height: u8,
    // number of values in current node.
    pub(super) n_values: u8,
    // padding to align to single word
    pub(super) padding: UnsafeCell<[u8; 4]>,

    /* fourth word */
    // The actual data is allocated just after the 0-sized data array.
    pub(super) data: UnsafeCell<[u8; 0]>,
}

impl NodeTemplate {
    /// Create a new node with given height, number of values and node kind.
    #[inline]
    pub unsafe fn new(height: u8, n_values: u8, kind: NodeKind) -> Owned<NodeTemplate> {
        debug_assert!(
            kind != NodeKind::Empty && kind != NodeKind::Leaf,
            "Invalid node kind"
        );
        debug_assert!(n_values <= 32, "Number of values exceeds limitation");
        let size_of_partial_keys = match kind {
            NodeKind::SP8 => 1 * n_values as usize,
            NodeKind::SP16 => 2 * n_values as usize,
            NodeKind::SP32 => 4 * n_values as usize,
            _ => todo!(),
        };
        let data_len = kind.mask_size()    // offset and mask
            + size_of_partial_keys                      // partial keys
        ;
        let data_len = ((data_len + 7) & !7)     // must align to 8 bytes
            + mem::size_of::<u64>() * n_values as usize // values
            ;
        debug_assert!(data_len <= 65535); // within bound of u16
        let data_len = data_len as u16;
        let mut node = Owned::<NodeTemplate>::new_dyn(data_len);
        node.version.store(0, Ordering::Relaxed);
        node.lock = RawRwLock::INIT;
        node.data_len = data_len;
        node.height = height;
        node.n_values = n_values;
        node.with_tag(kind as usize)
    }

    /// Create a new tuple id.
    #[inline]
    pub fn tid(tid: u64) -> Inline<NodeTemplate> {
        debug_assert!(tid.leading_zeros() >= <NodeTemplate>::ALIGN.trailing_zeros());
        Inline::new(tid as usize).with_tag(NodeKind::Leaf as usize)
    }

    #[inline]
    pub fn empty() -> Inline<NodeTemplate> {
        Inline::new(0).with_tag(NodeKind::Empty as usize)
    }
}

pub trait NodeOps {
    /// Returns reference of common template.
    fn tmpl(&self) -> &NodeTemplate;

    /* Common operations */

    #[inline]
    fn height(&self) -> u8 {
        self.tmpl().height
    }

    #[inline]
    fn n_values(&self) -> usize {
        self.tmpl().n_values as usize
    }
}

pub trait NodeSyncOps: NodeOps {
    type TargetMut;

    unsafe fn to_mut(&self) -> Self::TargetMut;

    /// Returns current version.
    #[inline]
    fn version(&self, order: Ordering) -> u64 {
        self.tmpl().version.load(order)
    }

    /// Increase the version.
    #[inline]
    fn promote_version(&self, order: Ordering) -> u64 {
        self.tmpl().version.fetch_add(1, order)
    }

    /// Check whether write lock is acquired.
    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        self.tmpl().lock.is_locked_exclusive()
    }

    /// Acquire a write lock.
    #[inline]
    fn lock_exclusive(&self) {
        self.tmpl().lock.lock_exclusive()
    }

    /// Acquire a read lock.
    #[inline]
    fn lock_shared(&self) {
        self.tmpl().lock.lock_shared()
    }

    /// Release a write lock.
    #[inline]
    unsafe fn unlock_exclusive(&self) {
        self.tmpl().lock.unlock_exclusive()
    }

    /// Release a read lock.
    #[inline]
    unsafe fn unlock_shared(&self) {
        self.tmpl().lock.unlock_shared()
    }

    /// Try optimistic read and return result if succeeds.
    fn try_opt_read<'s, U, F>(&'s self, f: F) -> Option<U>
    where
        U: 's,
        F: Fn(u64, &Self) -> U,
    {
        if self.is_locked_exclusive() {
            return None;
        }
        let pre_ver = self.version(Ordering::Acquire);
        let res = f(pre_ver, self);
        if self.is_locked_exclusive() || pre_ver != self.version(Ordering::Acquire) {
            return None;
        }
        Some(res)
    }

    fn unchecked_read<U, F>(&self, f: F) -> U
    where
        F: Fn(&Self) -> U,
        Self: Sized,
    {
        f(self)
    }

    fn with_read_lock<U, F>(&self, f: F) -> (u64, U)
    where
        F: FnOnce(&Self) -> U,
        Self: Sized,
    {
        self.lock_shared();
        defer!(unsafe { self.unlock_shared() });

        let prev_ver = self.version(Ordering::Relaxed);
        let res = f(self);
        (prev_ver, res)
    }

    fn with_write_lock<F>(&self, f: F) -> u64
    where
        F: FnOnce(Self::TargetMut),
        Self: Sized,
    {
        self.lock_exclusive();
        defer!(unsafe { self.unlock_exclusive() });
        // SAFETY:
        //
        // This is safe because we already acquired write lock on this node.
        let tgt = unsafe { self.to_mut() };
        f(tgt);
        let prev_ver = self.promote_version(Ordering::SeqCst);
        prev_ver
    }
}

pub trait NodeReadDataOps {
    type PartialKey: Copy;

    fn partial_key(&self, index: usize) -> Self::PartialKey;

    fn value(&self, index: usize) -> &Atomic<NodeTemplate>;
}

pub trait NodeWriteDataOps: NodeReadDataOps {
    fn set_partial_key(&mut self, index: usize, key: Self::PartialKey);

    fn value_mut(&mut self, index: usize) -> &mut Atomic<NodeTemplate>;

    unsafe fn value_mut_unchecked(
        &mut self,
        index: usize,
    ) -> &mut MaybeUninit<Atomic<NodeTemplate>>;
}

pub trait NodeSearchOps: NodeReadDataOps {
    fn extract_partial_key(&self, input: &[u8]) -> Self::PartialKey;

    /// Search given key in partial keys, and return its index if found.
    fn search_partial_key(&self, input: &[u8]) -> Option<usize>;
}

macro_rules! unchecked_read_node {
    ($id:ident, $e:expr) => {
        match $id.kind() {
            NodeKind::Empty | NodeKind::Leaf => panic!("invalid node type {:?}", $id.kind()),
            NodeKind::SP8 => {
                let sp8node = $crate::hot::node_impl::SP8NodeRef::from($id);
                sp8node.unchecked_read($e)
            }
            _ => todo!(),
        }
    };
}

macro_rules! try_opt_read_node {
    ($id:ident, $e:expr) => {
        match $id.kind() {
            NodeKind::Empty | NodeKind::Leaf => panic!("invalid node type {:?}", $id.kind()),
            NodeKind::SP8 => {
                let sp8node = unsafe { $crate::hot::node_impl::SP8NodeRef::from($id) };
                sp8node.try_opt_read($e)
            }
            _ => todo!(),
        }
    };
}

macro_rules! with_read_lock_node {
    ($id:ident, $e:expr) => {
        match $id.kind() {
            NodeKind::Empty | NodeKind::Leaf => panic!("invalid node type {:?}", $id.kind()),
            NodeKind::SP8 => {
                let sp8node = unsafe { $crate::hot::node_impl::SP8NodeRef::from($id) };
                sp8node.with_read_lock($e)
            }
            _ => todo!(),
        }
    };
}

macro_rules! with_write_lock_node {
    ($id:ident, $e:expr) => {
        match $id.kind() {
            NodeKind::Empty | NodeKind::Leaf => panic!("invalid node type {:?}", $id.kind()),
            NodeKind::SP8 => {
                let sp8node = unsafe { $crate::hot::node_impl::SP8NodeRef::from($id) };
                sp8node.with_write_lock($e)
            }
            _ => todo!(),
        }
    };
}

impl Shared<'_, NodeTemplate> {
    #[inline]
    pub fn height(&self) -> u8 {
        match self.kind() {
            NodeKind::Empty | NodeKind::Leaf => 0,
            _ => unsafe { self.deref().height },
        }
    }

    /// Returns whether the node is leaf.
    #[inline]
    pub fn kind(&self) -> NodeKind {
        NodeKind::from(self.tag() as u8)
    }

    #[inline]
    pub fn tid(&self) -> u64 {
        let (p, _) = self.decompose();
        p as u64 >> 3
    }
}

impl Pointable for NodeTemplate {
    const ALIGN: usize = mem::align_of::<Self>();
    type Init = u16;

    unsafe fn init(data_len: u16) -> *mut () {
        let size = mem::size_of::<Self>() + data_len as usize;
        let align = mem::align_of::<Self>();
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        let ptr = alloc::alloc(layout).cast::<Self>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr.cast()
    }

    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        &*(ptr as *const Self)
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        &mut *(ptr as *mut Self)
    }

    unsafe fn drop(ptr: *mut (), tag: usize) {
        // check the tag to identify the node kind.
        // If node is a branch node, drop its children first.
        match NodeKind::from(tag as u8) {
            NodeKind::Empty | NodeKind::Leaf => return,
            // There is no responsibility of parent to drop children.
            // The trie implementation should take care of recursive logic.
            _ => (),
        }
        let node = &*ptr.cast::<Self>();
        let size = mem::size_of::<Self>() + node.data_len as usize;
        let align = mem::align_of::<Self>();
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        alloc::dealloc(ptr.cast::<u8>(), layout);
    }
}

impl fmt::Debug for NodeTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("data_len", &self.data_len)
            .field("height", &self.height)
            .field("n_values", &self.n_values)
            .finish()
    }
}

impl Owned<NodeTemplate> {
    /// Returns whether the node is leaf.
    #[inline]
    pub fn kind(&self) -> NodeKind {
        NodeKind::from(self.tag() as u8)
    }

    #[inline]
    pub fn tid(&self) -> u64 {
        let (p, _) = self.decompose();
        p as u64 >> 3
    }
}
