use crate::bitmap::Bitmap;
use crate::error::{Error, Result};
use crate::repr::{ByteRepr, LeadingNonZeroByte, SMARepr};
use crate::sel::Sel;
use smallvec::SmallVec;
use std::ops::Index;
use std::sync::Arc;

#[derive(Debug)]
pub struct SMA {
    pub min: SmallVec<[u8; 16]>,
    pub max: SmallVec<[u8; 16]>,
    pub kind: PosKind,
    pos: PosTbl,
}

impl SMA {
    /// Create a new SMA.
    pub fn new(
        min: SmallVec<[u8; 16]>,
        max: SmallVec<[u8; 16]>,
        kind: PosKind,
        pos: PosTbl,
    ) -> Self {
        SMA {
            min,
            max,
            kind,
            pos,
        }
    }

    /// Build new SMA with input data.
    #[inline]
    pub fn build<T: ByteRepr + Ord + SMARepr>(data: &[T]) -> Self {
        assert!(!data.is_empty() && data.len() < u16::MAX as usize);
        // first iteration is to find min and max
        let (min, max) = min_max(data[0], data[0], data);
        // check the domain size
        let dom_size = max.wrap_sub(min).to_u64();
        let data = data.iter().cloned().enumerate();
        let (kind, pos) = if dom_size.to_u64() <= u8::MAX as u64 {
            // build with 1-byte delta lookup
            build_lookup1(min, data)
        } else if dom_size <= u16::MAX as u64 {
            // build with 2-byte delta lookup
            build_lookup2(min, data)
        } else if dom_size <= u32::MAX as u64 {
            // build with 4-byte delta lookup
            build_lookup4(min, data)
        } else {
            // build with 8-byte delta lookup
            build_lookup8(min, data)
        };
        let min = min.to_bytes();
        let max = max.to_bytes();
        SMA {
            min,
            max,
            kind,
            pos,
        }
    }

    #[inline]
    fn with_bitmap_validity<T: ByteRepr + Ord + SMARepr>(
        data: &[T],
        validity: &Bitmap,
    ) -> Option<Self> {
        let (mut min, mut max) = if let Some(idx) = validity.first_true() {
            (data[idx], data[idx])
        } else {
            // All values are null, so we cannot build SMA.
            return None;
        };
        {
            let mut idx = 0;
            for (valid, n) in validity.range_iter() {
                if valid {
                    (min, max) = min_max(min, max, &data[idx..idx + n]);
                }
                idx += n;
            }
        }
        let dom_size = max.wrap_sub(min).to_u64();
        let data = validity.true_index_iter().map(|idx| (idx, data[idx]));
        let (kind, pos) = if dom_size <= u8::MAX as u64 {
            // build with 1-byte delta lookup
            build_lookup1(min, data)
        } else if dom_size <= u16::MAX as u64 {
            // build with 2-byte delta lookup
            build_lookup2(min, data)
        } else if dom_size <= u32::MAX as u64 {
            // build with 4-byte delta lookup
            build_lookup4(min, data)
        } else {
            // build with 8-byte delta lookup
            build_lookup8(min, data)
        };
        let min = min.to_bytes();
        let max = max.to_bytes();
        Some(SMA {
            min,
            max,
            kind,
            pos,
        })
    }

    #[inline]
    pub fn with_validity<T: ByteRepr + Ord + SMARepr>(data: &[T], validity: &Sel) -> Option<Self> {
        match validity {
            Sel::None(_) => None,
            Sel::All(_) => Some(Self::build(data)),
            Sel::Index { count, indexes, .. } => {
                let (mut min, mut max) = (data[indexes[0] as usize], data[indexes[0] as usize]);
                let indexes = &indexes[..*count as usize];
                for idx in indexes {
                    let idx = *idx as usize;
                    min = data[idx].min(min);
                    max = data[idx].max(max);
                }
                let dom_size = max.wrap_sub(min).to_u64();
                let data = indexes
                    .iter()
                    .map(|idx| (*idx as usize, data[*idx as usize]));
                let (kind, pos) = if dom_size <= u8::MAX as u64 {
                    // build with 1-byte delta lookup
                    build_lookup1(min, data)
                } else if dom_size <= u16::MAX as u64 {
                    // build with 2-byte delta lookup
                    build_lookup2(min, data)
                } else if dom_size <= u32::MAX as u64 {
                    // build with 4-byte delta lookup
                    build_lookup4(min, data)
                } else {
                    // build with 8-byte delta lookup
                    build_lookup8(min, data)
                };
                let min = min.to_bytes();
                let max = max.to_bytes();
                Some(SMA {
                    min,
                    max,
                    kind,
                    pos,
                })
            }
            Sel::Bitmap(bm) => Self::with_bitmap_validity(data, bm.as_ref()),
        }
    }

    /// Returns total bytes of min/max values.
    #[inline]
    pub fn val_bytes(&self) -> usize {
        self.min.len() + self.max.len()
    }

    /// Returns bytes of kind.
    #[inline]
    pub fn kind_bytes(&self) -> usize {
        1
    }

    /// Returns total bytes of position lookup table.
    #[inline]
    pub fn pos_bytes(&self) -> usize {
        self.pos.total_bytes()
    }

    /// Returns minimal value.
    #[inline]
    pub fn min_val<T: ByteRepr>(&self) -> T {
        T::from_bytes(&self.min)
    }

    /// Returns maximum value.
    #[inline]
    pub fn max_val<T: ByteRepr>(&self) -> T {
        T::from_bytes(&self.max)
    }

    /// Returns range of given value.
    #[inline]
    pub fn range<T: ByteRepr + Ord + SMARepr>(&self, val: T) -> (u16, u16) {
        let min = self.min_val::<T>();
        let max = self.max_val::<T>();
        if val < min || val > max {
            return (0, 0);
        }
        let delta = val.wrap_sub(min);
        match self.kind {
            PosKind::Entry256 => {
                let delta = delta.to_u8();
                self.pos[delta as usize]
            }
            PosKind::Entry512 => {
                let delta = delta.to_u16();
                let (bidx, pidx) = delta.leading_non_zero_byte();
                self.pos[bidx * 256 + pidx as usize]
            }
            PosKind::Entry1024 => {
                let delta = delta.to_u32();
                let (bidx, pidx) = delta.leading_non_zero_byte();
                self.pos[bidx * 256 + pidx as usize]
            }
            PosKind::Entry2048 => {
                let delta = delta.to_u64();
                let (bidx, pidx) = delta.leading_non_zero_byte();
                self.pos[bidx * 256 + pidx as usize]
            }
        }
    }

    /// Returns number of lookup slots.
    #[inline]
    pub fn n_slots(&self) -> usize {
        self.pos.n_slots()
    }

    /// Returns lookup table as raw bytes.
    #[inline]
    pub fn raw_pos_tbl(&self) -> &[u8] {
        self.pos.raw()
    }

    /// Returns lookup table.
    #[inline]
    pub fn pos_tbl(&self) -> &[(u16, u16)] {
        self.pos.table()
    }

    /// Clone self to owned with atomic reference.
    #[inline]
    pub fn clone_to_owned(this: &Arc<Self>) -> Arc<Self> {
        match &this.pos {
            PosTbl::Owned { .. } => Arc::clone(this),
            PosTbl::Borrowed { .. } => Arc::new(SMA {
                min: this.min.clone(),
                max: this.max.clone(),
                kind: this.kind,
                pos: this.pos.to_owned(),
            }),
        }
    }
}

#[inline]
fn min_max<T: Copy + Ord>(min: T, max: T, src: &[T]) -> (T, T) {
    src.iter()
        .fold((min, max), |(mn, mx), item| (mn.min(*item), mx.max(*item)))
}

#[inline]
fn build_lookup1<T, I>(min: T, data: I) -> (PosKind, PosTbl)
where
    T: ByteRepr + Ord + SMARepr,
    I: IntoIterator<Item = (usize, T)>,
{
    // build with 1-byte delta lookup
    let mut lookup = vec![(0u16, 0u16); 256];
    for (i, v) in data {
        let i = i as u16;
        let delta = v.wrap_sub(min).to_u8(); // guaranteed to be 0~255
        let slot = &mut lookup[delta as usize];
        if slot.1 == 0 {
            *slot = (i, i + 1);
        } else {
            slot.1 = i + 1;
        }
    }
    let pos = PosTbl::new_owned(lookup.into_boxed_slice());
    (PosKind::Entry256, pos)
}

#[inline]
fn build_lookup2<T, I>(min: T, data: I) -> (PosKind, PosTbl)
where
    T: ByteRepr + Ord + SMARepr,
    I: IntoIterator<Item = (usize, T)>,
{
    let mut lookup = vec![(0u16, 0u16); 512];
    for (i, v) in data {
        let i = i as u16;
        let delta = v.wrap_sub(min).to_u16();
        let (bidx, pidx) = delta.leading_non_zero_byte();
        let slot = &mut lookup[bidx * 256 + pidx as usize];
        if slot.1 == 0 {
            *slot = (i, i + 1);
        } else {
            slot.1 = i + 1;
        }
    }
    let pos = PosTbl::new_owned(lookup.into_boxed_slice());
    (PosKind::Entry512, pos)
}

#[inline]
fn build_lookup4<T, I>(min: T, data: I) -> (PosKind, PosTbl)
where
    T: ByteRepr + Ord + SMARepr,
    I: IntoIterator<Item = (usize, T)>,
{
    let mut lookup = vec![(0u16, 0u16); 1024];
    for (i, v) in data {
        let i = i as u16;
        let delta = v.wrap_sub(min).to_u32();
        let (bidx, pidx) = delta.leading_non_zero_byte();
        let slot = &mut lookup[bidx * 256 + pidx as usize];
        if slot.1 == 0 {
            *slot = (i, i + 1);
        } else {
            slot.1 = i + 1;
        }
    }
    let pos = PosTbl::new_owned(lookup.into_boxed_slice());
    (PosKind::Entry1024, pos)
}

#[inline]
fn build_lookup8<T, I>(min: T, data: I) -> (PosKind, PosTbl)
where
    T: ByteRepr + Ord + SMARepr,
    I: IntoIterator<Item = (usize, T)>,
{
    let mut lookup = vec![(0u16, 0u16); 2048];
    for (i, v) in data {
        let i = i as u16;
        let delta = v.wrap_sub(min).to_u64();
        let (bidx, pidx) = delta.leading_non_zero_byte();
        let slot = &mut lookup[bidx * 256 + pidx as usize];
        if slot.1 == 0 {
            *slot = (i, i + 1);
        } else {
            slot.1 = i + 1;
        }
    }
    let pos = PosTbl::new_owned(lookup.into_boxed_slice());
    (PosKind::Entry2048, pos)
}

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
#[derive(Debug)]
pub enum PosTbl {
    Borrowed {
        ptr: Arc<[u8]>,
        len: usize,
        start_bytes: usize,
    },
    Owned {
        inner: Box<[(u16, u16)]>,
    },
}

impl PosTbl {
    /// Create an owned lookup table.
    #[inline]
    pub fn new_owned(data: Box<[(u16, u16)]>) -> Self {
        PosTbl::Owned { inner: data }
    }

    /// Create a borrowed lookup table.
    #[inline]
    pub fn new_borrowed(ptr: Arc<[u8]>, len: usize, start_bytes: usize) -> Self {
        assert!(start_bytes + std::mem::size_of::<(u16, u16)>() * len <= ptr.len());
        PosTbl::Borrowed {
            ptr,
            len,
            start_bytes,
        }
    }

    /// Returns length of lookup table.
    #[inline]
    pub fn n_slots(&self) -> usize {
        match self {
            PosTbl::Borrowed { len, .. } => *len,
            PosTbl::Owned { inner } => inner.len(),
        }
    }

    /// Returns total bytes of lookup table
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.n_slots() * std::mem::size_of::<(u16, u16)>()
    }

    /// Returns the range lookup table.
    #[inline]
    pub fn table(&self) -> &[(u16, u16)] {
        match self {
            PosTbl::Owned { inner } => inner,
            PosTbl::Borrowed {
                ptr,
                len,
                start_bytes,
            } => {
                // SAFETY
                //
                // Borrowed lookup table is immutable and all invariants are guaranteed.
                unsafe {
                    let ptr = ptr.as_ptr().add(*start_bytes);
                    std::slice::from_raw_parts(ptr as *const (u16, u16), *len)
                }
            }
        }
    }

    /// Returns raw byte slice.
    #[inline]
    pub fn raw(&self) -> &[u8] {
        match self {
            PosTbl::Owned { inner } => {
                let len = inner.len();
                // SAFETY
                //
                // pointer and length are valid.
                unsafe {
                    std::slice::from_raw_parts(
                        inner.as_ptr() as *const u8,
                        len * std::mem::size_of::<(u16, u16)>(),
                    )
                }
            }
            PosTbl::Borrowed {
                ptr,
                len,
                start_bytes,
            } => unsafe {
                let ptr = ptr.as_ptr().add(*start_bytes);
                std::slice::from_raw_parts(ptr, *len * std::mem::size_of::<(u16, u16)>())
            },
        }
    }

    /// Returns an owned table.
    #[inline]
    pub fn to_owned(&self) -> Self {
        match self {
            PosTbl::Owned { inner } => PosTbl::Owned {
                inner: inner.clone(),
            },
            PosTbl::Borrowed { len, .. } => {
                let mut inner = Vec::with_capacity(*len);
                inner.extend_from_slice(self.table());
                PosTbl::Owned {
                    inner: inner.into_boxed_slice(),
                }
            }
        }
    }
}

impl Index<usize> for PosTbl {
    type Output = (u16, u16);
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.table()[index]
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PosKind {
    Entry256 = 1,
    Entry512 = 2,
    Entry1024 = 4,
    Entry2048 = 8,
}

impl PosKind {
    /// Returns number of slots.
    #[inline]
    pub fn n_slots(self) -> usize {
        self as usize * 256
    }
}

impl TryFrom<u8> for PosKind {
    type Error = Error;
    #[inline]
    fn try_from(src: u8) -> Result<PosKind> {
        let res = match src {
            1 => PosKind::Entry256,
            2 => PosKind::Entry512,
            4 => PosKind::Entry1024,
            8 => PosKind::Entry2048,
            _ => return Err(Error::InvalidFormat),
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrapping_arith_i32() {
        let i = i32::MAX;
        let j = -10;
        let r = i.wrapping_sub(j);
        println!("r={:?}", r);
        println!("r as u32={:?}", r as u32);

        let k = i32::MAX as u32 + 3;
        let r2 = j.wrapping_add(k as i32);
        println!("r2={:?}", r2);
        println!("r2 as u32={:?}", r2 as u32);
    }

    #[test]
    fn test_sma_build_multi_sizes() {
        // 1-byte
        let data: Vec<i32> = vec![1, 1, 2, 1, 3];
        let sma = SMA::build(&data);
        assert_eq!(1, sma.min_val::<i32>());
        assert_eq!(3, sma.max_val::<i32>());
        assert_eq!(256, sma.n_slots());
        assert_eq!((0, 0), sma.range(0i32));
        assert_eq!((0, 4), sma.range(1i32));
        assert_eq!((2, 3), sma.range(2i32));
        assert_eq!((4, 5), sma.range(3i32));
        // 2-byte
        let data: Vec<i32> = (0i32..1024).collect();
        let sma = SMA::build(&data);
        assert_eq!(0, sma.min_val::<i32>());
        assert_eq!(1023, sma.max_val::<i32>());
        assert_eq!(512, sma.n_slots());
        assert_eq!((0, 1), sma.range(0i32));
        assert_eq!((1, 2), sma.range(1i32));
        assert_eq!((3, 4), sma.range(3i32));
        assert_eq!((256, 512), sma.range(256i32));
        // 4-byte
        let data: Vec<i32> = vec![0x00, 0x0100, 0x010000, 0x01000000];
        let sma = SMA::build(&data);
        assert_eq!(0, sma.min_val::<i32>());
        assert_eq!(0x01000000, sma.max_val::<i32>());
        assert_eq!(1024, sma.n_slots());
        assert_eq!((0, 1), sma.range(0i32));
        assert_eq!((0, 0), sma.range(1i32));
        assert_eq!((1, 2), sma.range(0x0100i32));
        assert_eq!((2, 3), sma.range(0x010000i32));
        assert_eq!((0, 0), sma.range(0x020000i32));
        // 8-byte
        let data: Vec<i64> = vec![
            0x00i64,
            0x0100,
            0x010000,
            0x01000000,
            0x0100000000,
            0x010000000000,
        ];
        let sma = SMA::build(&data);
        assert_eq!(0, sma.min_val::<i64>());
        assert_eq!(0x010000000000, sma.max_val::<i64>());
        assert_eq!(2048, sma.n_slots());
        assert_eq!((0, 1), sma.range(0i64));
        assert_eq!((0, 0), sma.range(1i64));
        assert_eq!((1, 2), sma.range(0x0100i64));
        assert_eq!((2, 3), sma.range(0x010000i64));
        assert_eq!((0, 0), sma.range(0x020000i64));
        assert_eq!((3, 4), sma.range(0x01000000i64));
        assert_eq!((4, 5), sma.range(0x0100000000i64));
        assert_eq!((5, 6), sma.range(0x010000000000i64));
    }

    #[test]
    fn test_sma_build_with_validity() {
        // all null
        let data: Vec<i32> = vec![1, 1, 2, 1, 3];
        let validity = Sel::None(5);
        assert!(SMA::with_validity(&data, &validity).is_none());
        // 1-byte
        let data: Vec<i32> = vec![1, 1, 2, 1, 3];
        let validity = Sel::All(5);
        let sma = SMA::with_validity(&data, &validity).unwrap();
        assert_eq!(1, sma.min_val::<i32>());
        assert_eq!(3, sma.max_val::<i32>());
        assert_eq!(256, sma.n_slots());
        assert_eq!((0, 0), sma.range(0i32));
        assert_eq!((0, 4), sma.range(1i32));
        assert_eq!((2, 3), sma.range(2i32));
        assert_eq!((4, 5), sma.range(3i32));
        // 2-byte
        let data: Vec<i32> = (0i32..1024).collect();
        let validity = Sel::Bitmap(Arc::new(Bitmap::from_iter(
            std::iter::repeat(true).take(1024),
        )));
        let sma = SMA::with_validity(&data, &validity).unwrap();
        assert_eq!(0, sma.min_val::<i32>());
        assert_eq!(1023, sma.max_val::<i32>());
        assert_eq!(512, sma.n_slots());
        assert_eq!((0, 1), sma.range(0i32));
        assert_eq!((1, 2), sma.range(1i32));
        assert_eq!((3, 4), sma.range(3i32));
        assert_eq!((256, 512), sma.range(256i32));
        // 4-byte
        let data: Vec<i32> = vec![0x00, 0x0100, 0x010000, 0x01000000];
        let validity = Sel::Bitmap(Arc::new(Bitmap::from_iter(vec![true; 4])));
        let sma = SMA::with_validity(&data, &validity).unwrap();
        assert_eq!(0, sma.min_val::<i32>());
        assert_eq!(0x01000000, sma.max_val::<i32>());
        assert_eq!(1024, sma.n_slots());
        assert_eq!((0, 1), sma.range(0i32));
        assert_eq!((0, 0), sma.range(1i32));
        assert_eq!((1, 2), sma.range(0x0100i32));
        assert_eq!((2, 3), sma.range(0x010000i32));
        assert_eq!((0, 0), sma.range(0x020000i32));
        // 8-byte
        let data: Vec<i64> = vec![
            0x00i64,
            0x0100,
            0x010000,
            0x01000000,
            0x0100000000,
            0x010000000000,
        ];
        let validity = Sel::Bitmap(Arc::new(Bitmap::from_iter(vec![true; 6])));
        let sma = SMA::with_validity(&data, &validity).unwrap();
        assert_eq!(0, sma.min_val::<i64>());
        assert_eq!(0x010000000000, sma.max_val::<i64>());
        assert_eq!(2048, sma.n_slots());
        assert_eq!((0, 1), sma.range(0i64));
        assert_eq!((0, 0), sma.range(1i64));
        assert_eq!((1, 2), sma.range(0x0100i64));
        assert_eq!((2, 3), sma.range(0x010000i64));
        assert_eq!((0, 0), sma.range(0x020000i64));
        assert_eq!((3, 4), sma.range(0x01000000i64));
        assert_eq!((4, 5), sma.range(0x0100000000i64));
        assert_eq!((5, 6), sma.range(0x010000000000i64));
    }
}
