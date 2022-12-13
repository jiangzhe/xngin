use super::node::{
    NodeKind, NodeOps, NodeReadDataOps, NodeSearchOps, NodeSyncOps, NodeTemplate, NodeWriteDataOps,
};
use crate::epoch::{Atomic, Owned, Shared};
use crate::hot::partial_key::PartialKey;
use std::arch::x86_64::_pext_u64;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ptr;

#[repr(C, align(8))]
pub struct SingleMaskNodeRef<'a, P: PartialKey> {
    tmpl: &'a NodeTemplate,
    _marker: PhantomData<P>,
}

#[repr(C, align(8))]
pub struct SingleMaskNodeMut<'a, P: PartialKey> {
    tmpl: &'a mut NodeTemplate,
    _marker: PhantomData<P>,
}

impl<P: PartialKey> SingleMaskNodeRef<'_, P> {
    #[inline]
    pub fn data_ptr(&self) -> *mut u8 {
        self.tmpl.data.get() as *mut u8
    }

    #[inline]
    pub fn mask_ptr(&self) -> *mut u64 {
        self.data_ptr() as *mut u64
    }

    #[inline]
    fn partial_keys_ptr(&self) -> *mut u8 {
        unsafe { self.data_ptr().add(8) }
    }

    #[inline]
    fn values_ptr(&self) -> *mut u8 {
        let n_partial_keys_bytes = mem::size_of::<P>() * self.n_values();
        let values_offset = 8 + n_partial_keys_bytes;
        let aligned_offset = (values_offset + 7) & !7;
        unsafe { self.data_ptr().add(aligned_offset) }
    }

    #[inline]
    fn msb_offset_ptr(&self) -> *mut u16 {
        self.tmpl.padding.get() as *mut u16
    }

    /// Offset is 2-byte integer which is embedded in header.
    #[inline]
    pub fn msb_offset(&self) -> u16 {
        let p = self.msb_offset_ptr() as *const u16;
        unsafe { p.read() }
    }

    #[inline]
    fn mask(&self) -> u64 {
        unsafe {
            let p = self.mask_ptr() as *const u8 as *const u64;
            p.read()
        }
    }
}

#[allow(dead_code)]
impl<P: PartialKey> SingleMaskNodeMut<'_, P> {
    #[inline]
    pub fn to_ref(&self) -> SingleMaskNodeRef<'_, P> {
        SingleMaskNodeRef {
            tmpl: self.tmpl,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn data_ptr(&self) -> *mut u8 {
        self.to_ref().data_ptr()
    }

    #[inline]
    pub fn mask_ptr(&self) -> *mut u64 {
        self.to_ref().mask_ptr()
    }

    #[inline]
    fn partial_keys_ptr(&self) -> *mut u8 {
        self.to_ref().partial_keys_ptr()
    }

    #[inline]
    fn values_ptr(&self) -> *mut u8 {
        self.to_ref().values_ptr()
    }

    #[inline]
    pub fn msb_offset(&self) -> u16 {
        self.to_ref().msb_offset()
    }

    #[inline]
    pub fn set_msb_offset(&mut self, msb_offset: u16) {
        let p = self.to_ref().msb_offset_ptr();
        unsafe { p.write(msb_offset) }
    }

    #[inline]
    fn mask(&self) -> u64 {
        self.to_ref().mask()
    }

    #[inline]
    pub fn set_mask(&mut self, mask: u64) {
        let p = self.to_ref().mask_ptr();
        unsafe { p.write(mask) }
    }
}

impl<P: PartialKey> NodeOps for SingleMaskNodeRef<'_, P> {
    #[inline]
    fn tmpl(&self) -> &NodeTemplate {
        self.tmpl
    }
}

impl<'g, P: PartialKey> NodeSyncOps for SingleMaskNodeRef<'g, P> {
    type TargetMut = SingleMaskNodeMut<'g, P>;

    #[allow(clippy::cast_ref_to_mut)]
    #[inline]
    unsafe fn as_mut(&self) -> Self::TargetMut {
        let tmpl = &mut *(self.tmpl as *const _ as *mut _);
        SingleMaskNodeMut {
            tmpl,
            _marker: PhantomData,
        }
    }
}

impl<P: PartialKey> NodeOps for SingleMaskNodeMut<'_, P> {
    #[inline]
    fn tmpl(&self) -> &NodeTemplate {
        self.tmpl
    }
}

impl<P: PartialKey> NodeReadDataOps for SingleMaskNodeRef<'_, P> {
    type PartialKey = P;

    fn partial_key(&self, index: usize) -> Self::PartialKey {
        debug_assert!(index < self.n_values());
        let p = self.partial_keys_ptr() as *const Self::PartialKey;
        unsafe { *p.add(index) }
    }

    fn value(&self, index: usize) -> &Atomic<NodeTemplate> {
        debug_assert!(index < self.n_values());
        let p = self.values_ptr() as *const Atomic<NodeTemplate>;
        unsafe { &*p.add(index) }
    }
}

impl<P: PartialKey> NodeReadDataOps for SingleMaskNodeMut<'_, P> {
    type PartialKey = P;

    fn partial_key(&self, index: usize) -> Self::PartialKey {
        self.to_ref().partial_key(index)
    }

    fn value(&self, index: usize) -> &Atomic<NodeTemplate> {
        let p = self.values_ptr() as *const Atomic<NodeTemplate>;
        unsafe { &*p.add(index) }
    }
}

impl<P: PartialKey> NodeWriteDataOps for SingleMaskNodeMut<'_, P> {
    fn set_partial_key(&mut self, index: usize, key: Self::PartialKey) {
        debug_assert!(index < self.n_values());
        let p = self.partial_keys_ptr() as *mut Self::PartialKey;
        unsafe { p.add(index).write(key) }
    }

    fn value_mut(&mut self, index: usize) -> &mut Atomic<NodeTemplate> {
        debug_assert!(index < self.n_values());
        unsafe { self.value_mut_unchecked(index).assume_init_mut() }
    }

    unsafe fn value_mut_unchecked(
        &mut self,
        index: usize,
    ) -> &mut MaybeUninit<Atomic<NodeTemplate>> {
        let p = self.values_ptr() as *mut MaybeUninit<Atomic<NodeTemplate>>;
        &mut *p.add(index)
    }
}

impl<P: PartialKey> NodeSearchOps for SingleMaskNodeRef<'_, P> {
    fn extract_partial_key(&self, input: &[u8]) -> Self::PartialKey {
        let start = self.msb_offset() as usize / 8;
        let len = (input.len() - start).min(8);
        // partial key must reside within 8-bytes,
        // so we fix the key size with 8.
        let mut key_u64 = [0u8; 8];
        key_u64[..len].copy_from_slice(&input[start..start + len]);
        let mask = self.mask();
        unsafe {
            let key = _pext_u64(u64::from_be_bytes(key_u64), mask);
            P::cast_from(key)
        }
    }

    /// Search given key in partial keys, and return its index if found.
    fn search_partial_key(&self, input: &[u8]) -> Option<usize> {
        let partial_key = self.extract_partial_key(input);
        let step = 32 / mem::size_of::<P>();
        let mut i = 0;
        let mut keys_ptr = self.partial_keys_ptr() as *const P;
        unsafe {
            let end_ptr = keys_ptr.add(self.n_values());
            while keys_ptr.add(step) <= end_ptr {
                let match_mask = partial_key.mm256_search(keys_ptr);
                // if we always make keys continugous, we do not need the entries mask.
                // let res_mask = (match_mask << i) & self.header.used_entries_mask;
                if match_mask != 0 {
                    return Some(match_mask.trailing_zeros() as usize + i);
                }
                keys_ptr = keys_ptr.add(step);
                i += step;
            }
            // use common comparison for remained keys
            while keys_ptr < end_ptr {
                let pk = *keys_ptr;
                if pk == partial_key {
                    return Some(i);
                }
                keys_ptr = keys_ptr.add(1);
                i += 1;
            }
            None
        }
    }
}

impl<P: PartialKey> Drop for SingleMaskNodeRef<'_, P> {
    fn drop(&mut self) {
        for i in 0..self.n_values() {
            unsafe {
                ptr::drop_in_place(self.value(i as usize) as *const _ as *mut Atomic<NodeTemplate>)
            }
        }
    }
}

/// SingleMask + u8 partial key array.
/// header + u16 offset + u64 mask + n * u8 partial keys + n * u64 values
pub type SP8NodeRef<'a> = SingleMaskNodeRef<'a, u8>;

impl<'g> SP8NodeRef<'g> {
    // This method is unsafe because the returned value can read underlying
    // fields without synchronization.
    #[inline]
    pub unsafe fn from(shared: Shared<'g, NodeTemplate>) -> Self {
        debug_assert_eq!(shared.kind(), NodeKind::SP8);
        let tmpl = shared.deref();
        Self {
            tmpl,
            _marker: PhantomData,
        }
    }
}

pub type SP8NodeMut<'a> = SingleMaskNodeMut<'a, u8>;

#[allow(dead_code)]
impl<'a> SP8NodeMut<'a> {
    #[inline]
    pub fn from(owned: &'a mut Owned<NodeTemplate>) -> Self {
        debug_assert_eq!(owned.kind(), NodeKind::SP8);
        let tmpl = &mut **owned;
        Self {
            tmpl,
            _marker: PhantomData,
        }
    }
}

pub type SP16Node<'a> = SingleMaskNodeRef<'a, u16>;

pub type SP32Node<'a> = SingleMaskNodeRef<'a, u32>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch;
    use crate::hot::node::NodeKind;
    use crate::hot::node::NodeTemplate;
    use memoffset::offset_of;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_node_impl_size_and_align() {
        println!("size of SingleMaskU8Node={}", mem::size_of::<SP8NodeRef>());
        println!("size of SingleMaskU16Node={}", mem::size_of::<SP16Node>());
        println!("size of SingleMaskU32Node={}", mem::size_of::<SP32Node>());

        let offset_tmpl = offset_of!(NodeTemplate, data);
        assert!(offset_tmpl % 8 == 0);
    }

    #[test]
    fn test_node_impl_new() {
        let guard = epoch::pin();

        let mut node = unsafe { NodeTemplate::new(1, 2, NodeKind::SP8) };
        assert_eq!(NodeKind::SP8, node.kind());
        {
            let mut node = unsafe { SP8NodeMut::from(&mut node) };
            assert_eq!(1, node.height());
            assert_eq!(2, node.n_values());

            node.set_partial_key(1, 10);
            node.set_partial_key(0, 20);

            assert_eq!(20, node.partial_key(0));
            assert_eq!(10, node.partial_key(1));

            unsafe {
                node.value_mut_unchecked(0)
                    .write(Atomic::from(NodeTemplate::tid(100)));
                node.value_mut_unchecked(1)
                    .write(Atomic::from(NodeTemplate::tid(200)));
            }

            let v0 = node.value(0).load(Ordering::Relaxed, &guard).tid();
            assert_eq!(100, v0);
            let v1 = node.value(1).load(Ordering::Relaxed, &guard).tid();
            assert_eq!(200, v1);
        }
        assert_eq!(2, node.tag());
        let node = node.with_tag(3);
        assert_eq!(3, node.tag());
        let node = node.with_tag(2);
        assert_eq!(2, node.tag());
    }

    #[test]
    fn test_node_impl_tid() {
        let node = NodeTemplate::tid(123);
        assert_eq!(123, node.value())
    }
}
