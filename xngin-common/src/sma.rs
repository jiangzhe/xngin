use crate::bitmap::{bitmap_first_true, bitmap_range_iter};
use smallvec::SmallVec;
use std::ops::Index;
use std::sync::Arc;

#[derive(Debug)]
pub struct SMA {
    pub min: SmallVec<[u8; 16]>,
    pub max: SmallVec<[u8; 16]>,
    pub pos: PosTbl,
}

impl SMA {
    /// Returns total bytes of min/max values.
    #[inline]
    pub fn val_bytes(&self) -> usize {
        self.min.len() + self.max.len()
    }

    /// Returns total bytes of position lookup table.
    #[inline]
    pub fn pos_bytes(&self) -> usize {
        self.pos.total_bytes()
    }

    #[inline]
    pub fn range_i32(&self, val: i32) -> (u16, u16) {
        let min = i32::from_ne_bytes(self.min[..4].try_into().unwrap());
        if val < min {
            return (0, 0);
        }
        let max = i32::from_ne_bytes(self.max[..4].try_into().unwrap());
        if val > max {
            return (0, 0);
        }
        let delta = val.wrapping_sub(min) as u32;
        if delta <= u8::MAX as u32 {
            self.pos[delta as usize]
        } else {
            todo!()
        }
    }

    #[inline]
    pub fn build_from_i32s(bm: Option<&[u64]>, data: &[i32]) -> Option<Self> {
        if let Some(bm) = bm {
            let (mut min, mut max) = if let Some(idx) = bitmap_first_true(bm, data.len()) {
                (data[idx], data[idx])
            } else {
                return None;
            };
            let mut idx = 0;
            for (valid, n) in bitmap_range_iter(bm, data.len()) {
                if valid {
                    (min, max) = min_max_i32s(min, max, &data[idx..idx + n]);
                }
                idx += n;
            }
            let dom_size = max.wrapping_sub(min) as u32;
            let pos = if dom_size <= u8::MAX as u32 {
                let mut lookup = vec![(0u16, 0u16); 256];
                let mut idx = 0;
                for (valid, n) in bitmap_range_iter(bm, data.len()) {
                    if valid {
                        for (i, v) in data[idx..idx + n].iter().enumerate() {
                            let i = (idx + i) as u16;
                            let delta = (v - min) as usize; // guaranteed to be 0~255
                            let slot = &mut lookup[delta];
                            if slot.1 == 0 {
                                *slot = (i, i + 1);
                            } else {
                                slot.1 = i + 1;
                            }
                        }
                    }
                    idx += n;
                }
                PosTbl::new_owned(lookup.into_boxed_slice())
            } else {
                todo!()
            };
            let min = min.to_ne_bytes().iter().cloned().collect();
            let max = max.to_ne_bytes().iter().cloned().collect();
            Some(SMA { min, max, pos })
        } else {
            Some(SMA::from(data))
        }
    }
}

impl From<&[i32]> for SMA {
    #[inline]
    fn from(src: &[i32]) -> Self {
        assert!(!src.is_empty() && src.len() < u16::MAX as usize);
        // first iteration is to find min and max
        let (min, max) = min_max_i32s(src[0], src[0], src);
        // check the domain size
        let dom_size = max.wrapping_sub(min) as u32;
        let pos = if dom_size <= u8::MAX as u32 {
            // build with 1-byte delta lookup
            let mut lookup = vec![(0u16, 0u16); 256];
            for (i, v) in src.iter().enumerate() {
                let i = i as u16;
                let delta = (v - min) as usize; // guaranteed to be 0~255
                let slot = &mut lookup[delta];
                if slot.1 == 0 {
                    *slot = (i, i + 1);
                } else {
                    slot.1 = i + 1;
                }
            }
            PosTbl::new_owned(lookup.into_boxed_slice())
        } else if dom_size <= u16::MAX as u32 {
            // build with 2-byte delta lookup
            todo!()
        } else {
            // build with 4-byte delta lookup
            unimplemented!()
        };
        let min = min.to_ne_bytes().iter().cloned().collect();
        let max = max.to_ne_bytes().iter().cloned().collect();
        SMA { min, max, pos }
    }
}

#[inline]
fn min_max_i32s(min: i32, max: i32, src: &[i32]) -> (i32, i32) {
    src.iter()
        .fold((min, max), |acc, item| (acc.0.min(*item), acc.1.max(*item)))
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
}

impl Index<usize> for PosTbl {
    type Output = (u16, u16);
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.table()[index]
    }
}

#[cfg(test)]
mod tests {
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
}
