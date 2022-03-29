use std::ops::Index;
use std::slice;

/// PSMA lookup table.
/// The general idea is to split each value into bytes,
/// and store the index range based on first non-zero byte
/// of each value.
/// If we lookup for specific value, we can lookup the
/// range in this table. then perform range scan over
/// the real data.
///
/// The index range is always two 160bit integer, indicating
/// the start and end(exclusive) of such value with identical
/// prefix.
pub struct ViewLookup {
    ptr: *const u8,
    len: usize,
}

/// todo: store Arc<[u8]> to ensure pointer is valid.
unsafe impl Send for ViewLookup {}
unsafe impl Sync for ViewLookup {}

impl ViewLookup {
    #[inline]
    fn table(&self) -> &[(u16, u16)] {
        // SAFETY
        //
        // User must ensure pointer and length are valid.
        unsafe { slice::from_raw_parts(self.ptr as *const (u16, u16), self.len) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Index<usize> for ViewLookup {
    type Output = (u16, u16);

    #[inline]
    fn index(&self, idx: usize) -> &Self::Output {
        &self.table()[idx]
    }
}

pub struct VecLookup {
    inner: Box<[(u16, u16)]>,
}

impl VecLookup {
    #[inline]
    pub fn new(tbl: Box<[(u16, u16)]>) -> Self {
        VecLookup { inner: tbl }
    }
}

impl VecLookup {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Index<usize> for VecLookup {
    type Output = (u16, u16);

    #[inline]
    fn index(&self, idx: usize) -> &Self::Output {
        &self.inner[idx]
    }
}
