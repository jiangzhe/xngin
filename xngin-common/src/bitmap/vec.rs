use crate::alloc::RawArray;
use crate::bitmap::view::ViewBitmap;
use crate::bitmap::{AppendBitmap, ReadBitmap, WriteBitmap};
use crate::error::{Error, Result};
use std::iter::FromIterator;
use std::ops::Range;

#[derive(Debug)]
pub struct VecBitmap {
    inner: RawArray,
    len_u1: usize,
}

/// keep capacity when cloning.
impl Clone for VecBitmap {
    #[inline]
    fn clone(&self) -> Self {
        // identical capacity to original Vec
        let cap_u8 = self.inner.cap_u8();
        let mut inner = RawArray::with_capacity(cap_u8);
        // inner.extend_from_slice(&self.inner);
        inner.as_slice_mut()[..cap_u8].copy_from_slice(self.inner.as_slice());
        Self {
            inner,
            len_u1: self.len_u1,
        }
    }
}

impl From<(&[u8], usize)> for VecBitmap {
    #[inline]
    fn from((src_bm, n_bits): (&[u8], usize)) -> Self {
        assert!(n_bits <= src_bm.len() * 8);
        let mut res = VecBitmap::with_capacity(n_bits);
        res.inner.as_slice_mut()[..src_bm.len()].copy_from_slice(src_bm);
        res.len_u1 = n_bits;
        res
    }
}

impl From<(&[u64], usize)> for VecBitmap {
    #[inline]
    fn from((src_bm, n_bits): (&[u64], usize)) -> Self {
        assert!(n_bits <= src_bm.len() * 64);
        let mut res = VecBitmap::with_capacity(n_bits);
        unsafe {
            res.inner
                .cast_slice_mut::<u64>(src_bm.len())
                .copy_from_slice(src_bm);
        }
        res.len_u1 = n_bits;
        res
    }
}

impl From<Vec<bool>> for VecBitmap {
    #[inline]
    fn from(src: Vec<bool>) -> Self {
        let mut bm = Self::with_capacity(src.len());

        bm.len_u1 = src.len();
        let chunks = src.chunks_exact(8);
        if !chunks.remainder().is_empty() {
            // remainder not empty, handle it
            let last_u = {
                let mut u = 0u8;
                for (i, &b) in chunks.remainder().iter().enumerate() {
                    u |= if b { 1 << i } else { 0 };
                }
                u
            };
            let pos = src.len() / 8;
            bm.inner.as_slice_mut()[pos] = last_u;
        }
        for (chunk, b) in chunks.zip(bm.inner.as_slice_mut()) {
            // SAFETY:
            // chunk always has 8 bools so use get_unchecked to avoid bound check
            unsafe {
                let v0: u8 = if *chunk.get_unchecked(0) { 1 } else { 0 };
                let v1: u8 = if *chunk.get_unchecked(1) { 2 } else { 0 };
                let v2: u8 = if *chunk.get_unchecked(2) { 4 } else { 0 };
                let v3: u8 = if *chunk.get_unchecked(3) { 8 } else { 0 };
                let v4: u8 = if *chunk.get_unchecked(4) { 16 } else { 0 };
                let v5: u8 = if *chunk.get_unchecked(5) { 32 } else { 0 };
                let v6: u8 = if *chunk.get_unchecked(6) { 64 } else { 0 };
                let v7: u8 = if *chunk.get_unchecked(7) { 128 } else { 0 };
                *b = v0 | v1 | v2 | v3 | v4 | v5 | v6 | v7
            }
        }
        bm
    }
}

impl Default for VecBitmap {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl VecBitmap {
    /// Create a new bitmap
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    #[inline]
    pub fn with_capacity(cap_u1: usize) -> Self {
        let cap_u8 = if cap_u1 == 0 { 1 } else { (cap_u1 + 7) / 8 };
        VecBitmap {
            inner: RawArray::with_capacity(cap_u8),
            len_u1: 0,
        }
    }

    #[inline]
    pub fn from_view(view: &ViewBitmap) -> Self {
        let (bm, len) = view.aligned_u64s();
        VecBitmap::from((bm, len))
    }

    #[inline]
    pub fn reserve(&mut self, cap_u1: usize) {
        self.inner.reserve((cap_u1 + 7) / 8);
    }
}

impl ReadBitmap for VecBitmap {
    #[inline]
    fn aligned_u64s(&self) -> (&[u64], usize) {
        let len_u64 = (self.len_u1 + 63) / 64;
        (
            unsafe { self.inner.cast_slice::<u64>(len_u64) },
            self.len_u1,
        )
    }

    #[inline]
    fn len(&self) -> usize {
        self.len_u1
    }
}

impl WriteBitmap for VecBitmap {
    #[inline]
    fn aligned_u64s_mut(&mut self) -> (&mut [u64], usize) {
        let len_u64 = (self.len_u1 + 63) / 64;
        (
            unsafe { self.inner.cast_slice_mut::<u64>(len_u64) },
            self.len_u1,
        )
    }

    #[inline]
    fn clear(&mut self) {
        self.len_u1 = 0;
    }

    #[inline]
    fn set_len(&mut self, len_u1: usize) -> Result<()> {
        if len_u1 <= self.inner.cap_u8() * 8 {
            self.len_u1 = len_u1;
            Ok(())
        } else {
            Err(Error::IndexOutOfBound(format!(
                "set bitmap length greater than capacity {} > {}",
                len_u1,
                self.inner.cap_u8() * 8
            )))
        }
    }
}

impl AppendBitmap for VecBitmap {
    #[inline]
    fn add(&mut self, val: bool) -> Result<()> {
        self.reserve(self.len_u1 + 1);
        let bm = self.inner.as_slice_mut();
        if val {
            bm[self.len_u1 >> 3] |= 1 << (self.len_u1 & 7);
        } else {
            bm[self.len_u1 >> 3] &= !(1 << (self.len_u1 & 7));
        }
        self.len_u1 += 1;
        Ok(())
    }

    #[inline]
    fn extend<T: ReadBitmap + ?Sized>(&mut self, that: &T) -> Result<()> {
        let (tbm, tlen) = that.aligned_u64s();
        // reserve space to make sure capacity is still multiple of 8
        let orig_len = self.len();
        let tgt_len = orig_len + tlen;
        self.reserve(tgt_len);
        let tgt_len_u64 = (tgt_len + 63) / 64;
        let dst = unsafe { self.inner.cast_slice_mut::<u64>(tgt_len_u64) };
        super::copy_bits(dst, orig_len, tbm, tlen);
        self.set_len(tgt_len)
    }

    #[inline]
    fn extend_range<T: ReadBitmap + ?Sized>(
        &mut self,
        that: &T,
        range: Range<usize>,
    ) -> Result<()> {
        let (tbm, tlen) = that.aligned_u64s();
        if range.end > tlen {
            return Err(Error::IndexOutOfBound(format!(
                "extend range {:?} larger than column length {}",
                range, tlen
            )));
        }
        let range_len = range.end - range.start;
        let orig_len = self.len();
        let tgt_len = orig_len + range_len;
        self.reserve(tgt_len);
        let tgt_len_u64 = (tgt_len + 63) / 64;
        let dst = unsafe { self.inner.cast_slice_mut::<u64>(tgt_len_u64) };
        super::copy_bits_range(dst, orig_len, tbm, range);
        self.set_len(tgt_len)
    }

    #[inline]
    fn extend_const(&mut self, val: bool, len: usize) -> Result<()> {
        let orig_len = self.len();
        let tgt_len = orig_len + len;
        self.reserve(tgt_len);
        let bm = self.inner.as_slice_mut();
        if val {
            super::copy_const_bits(bm, orig_len, true, len);
        } else {
            super::copy_const_bits(bm, orig_len, false, len);
        }
        self.set_len(tgt_len)
    }
}

impl FromIterator<bool> for VecBitmap {
    #[inline]
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = bool>,
    {
        let iter = iter.into_iter();
        match iter.size_hint() {
            (_, Some(hb)) => {
                // allocate according to high bound
                let mut bm = VecBitmap::with_capacity(((hb + 63) >> 6) << 6);
                bm.set_len(hb).unwrap(); // total number should be not greater than hb
                let mut i = 0usize; // record insert count
                iter.for_each(|v| {
                    bm.set(i, v).unwrap();
                    i += 1;
                });
                bm.set_len(i).unwrap(); // update insert count
                bm
            }
            (_, None) => {
                let mut bm = VecBitmap::new();
                iter.for_each(|v| bm.add(v).unwrap());
                bm
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::ReadBitmapExt;
    use rand::Rng;

    #[test]
    fn test_bitmap_simple() -> Result<()> {
        let mut bm = VecBitmap::with_capacity(10);
        assert_eq!(0, bm.len());
        assert!(bm.is_empty());
        bm.add(true)?;
        bm.add(false)?;
        bm.add(true)?;
        assert_eq!(3, bm.len());
        assert!(bm.get(0)?);
        assert!(!bm.get(1)?);
        assert!(bm.get(2)?);
        bm.clear();
        assert_eq!(0, bm.len());
        Ok(())
    }

    #[test]
    fn test_bitmap_from_u8s_and_len() {
        let bm = VecBitmap::from((&[5u8][..], 4usize));
        assert_eq!(4, bm.len());
        assert!(bm.get(0).unwrap());
        assert!(!bm.get(1).unwrap());
        assert!(bm.get(2).unwrap());
        assert!(!bm.get(3).unwrap());
    }

    #[test]
    fn test_bitmap_iter1() -> Result<()> {
        let mut bm = VecBitmap::new();
        for _ in 0..4 {
            bm.add(true)?;
        }
        assert_eq!((true, 4), bm.range_iter().next().unwrap());
        for _ in 0..4 {
            bm.add(true)?;
        }
        assert_eq!((true, 8), bm.range_iter().next().unwrap());
        for _ in 0..16 {
            bm.add(true)?;
        }
        assert_eq!((true, 24), bm.range_iter().next().unwrap());
        Ok(())
    }

    #[test]
    fn test_bitmap_iter2() -> Result<()> {
        let mut bm = VecBitmap::new();
        for _ in 0..4 {
            bm.add(false)?;
        }
        for _ in 0..2 {
            bm.add(true)?;
        }
        for _ in 0..4 {
            bm.add(true)?;
        }
        let mut iter = bm.range_iter();
        assert_eq!((false, 4), iter.next().unwrap());
        assert_eq!((true, 6), iter.next().unwrap());
        assert!(iter.next().is_none());
        Ok(())
    }

    #[test]
    fn test_bitmap_iter3() -> Result<()> {
        let mut bm = VecBitmap::new();
        for _ in 0..4 {
            bm.add(true)?;
        }
        for _ in 0..8 {
            bm.add(false)?;
        }
        let mut iter = bm.range_iter();
        assert_eq!((true, 4), iter.next().unwrap());
        assert_eq!((false, 8), iter.next().unwrap());
        assert!(iter.next().is_none());
        Ok(())
    }

    #[test]
    fn test_bitmap_iter4() -> Result<()> {
        let mut bm = VecBitmap::new();
        for _ in 0..7 {
            bm.add(false)?;
        }
        bm.add(true)?;
        let mut iter = bm.range_iter();
        assert_eq!((false, 7), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert!(iter.next().is_none());
        Ok(())
    }

    #[test]
    fn test_bitmap_iter5() -> Result<()> {
        let mut bm = VecBitmap::new();
        bm.add(true)?;
        for _ in 0..3 {
            bm.add(false)?;
        }
        bm.add(true)?;
        for _ in 0..3 {
            bm.add(false)?;
        }
        let mut iter = bm.range_iter();
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert!(iter.next().is_none());
        Ok(())
    }

    #[test]
    fn test_bitmap_iter6() -> Result<()> {
        let mut bm = VecBitmap::new();
        bm.add(true)?;
        for _ in 0..3 {
            bm.add(false)?;
        }
        bm.add(true)?;
        let mut iter = bm.range_iter();
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert!(iter.next().is_none());
        Ok(())
    }

    #[test]
    fn test_bitmap_iter7() -> Result<()> {
        let mut bm = VecBitmap::new();
        bm.add(true)?;
        for _ in 0..15 {
            bm.add(false)?;
        }
        bm.add(true)?;
        for _ in 0..3 {
            bm.add(false)?;
        }
        for _ in 0..7 {
            bm.add(true)?;
        }
        let mut iter = bm.range_iter();
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 15), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert_eq!((true, 7), iter.next().unwrap());
        assert!(iter.next().is_none());
        Ok(())
    }

    fn custom_bools(shift_n: usize) -> (Vec<bool>, Vec<bool>) {
        let bools = vec![
            true, false, true, true, false, false, true, false, true, true, true, false, false,
            true, false, true, true, false, true, true, false, false, true, false, true, true,
            true, false, false, true, false, true,
        ];
        let shifted = bools.iter().skip(shift_n).cloned().collect();
        (bools, shifted)
    }

    #[test]
    fn test_bitmap_shift1() -> Result<()> {
        const SHIFT_N: usize = 1;
        let mut bm = VecBitmap::new();
        let (bools, expected) = custom_bools(SHIFT_N);
        for v in &bools {
            bm.add(*v)?;
        }
        // shift
        bm.shift(SHIFT_N)?;
        let shifted: Vec<_> = bm.bools().collect();
        assert_eq!(expected, shifted);
        Ok(())
    }

    #[test]
    fn test_bitmap_shift2() -> Result<()> {
        const SHIFT_N: usize = 8;
        let mut bm = VecBitmap::new();
        let (bools, expected) = custom_bools(SHIFT_N);
        for v in &bools {
            bm.add(*v)?;
        }
        // shift
        bm.shift(SHIFT_N)?;
        let shifted: Vec<_> = bm.bools().collect();
        assert_eq!(expected, shifted);
        Ok(())
    }

    #[test]
    fn test_bitmap_shift3() -> Result<()> {
        const SHIFT_N: usize = 1;
        let mut bm = VecBitmap::new();
        let (bools, expected) = custom_bools(SHIFT_N);
        for v in &bools {
            bm.add(*v)?;
        }
        // shift
        bm.shift(SHIFT_N)?;
        let shifted: Vec<_> = bm.bools().collect();
        assert_eq!(expected, shifted);
        Ok(())
    }

    #[test]
    fn test_bitmap_shift4() -> Result<()> {
        const SHIFT_N: usize = 9;
        let mut bm = VecBitmap::new();
        let (bools, expected) = custom_bools(SHIFT_N);
        for v in &bools {
            bm.add(*v)?;
        }
        let mut bm1 = VecBitmap::new();
        for v in &expected {
            bm1.add(*v)?;
        }
        // shift
        bm.shift(SHIFT_N)?;
        let shifted: Vec<_> = bm.bools().collect();
        assert_eq!(expected, shifted);
        Ok(())
    }

    #[test]
    fn test_bitmap_shift5() -> Result<()> {
        const SHIFT_N: usize = 17;
        let mut bm = VecBitmap::new();
        let (bools, expected) = custom_bools(SHIFT_N);
        for v in &bools {
            bm.add(*v)?;
        }
        // shift
        bm.shift(SHIFT_N)?;
        let shifted: Vec<_> = bm.bools().collect();
        assert_eq!(expected, shifted);
        Ok(())
    }

    #[test]
    fn test_bitmap_shift6() -> Result<()> {
        const SHIFT_N: usize = 21;
        let mut bm = VecBitmap::new();
        let (bools, expected) = custom_bools(SHIFT_N);
        for v in &bools {
            bm.add(*v)?;
        }
        // shift
        bm.shift(SHIFT_N)?;
        let shifted: Vec<_> = bm.bools().collect();
        assert_eq!(expected, shifted);
        Ok(())
    }

    // generate random bitmaps and test shift operations
    #[test]
    fn test_bitmap_shift7() -> Result<()> {
        let mut rng = rand::thread_rng();
        let mut bm = VecBitmap::new();
        let mut bools: Vec<bool> = Vec::new();
        for _ in 0..256 {
            bm.clear();
            bools.clear();
            let size: usize = rng.gen_range(128..1024 * 10);
            for _ in 0..size {
                bools.push(rng.gen());
            }
            for b in &bools {
                bm.add(*b)?;
            }
            let bits = rng.gen_range(0..size - 127);
            bm.shift(bits)?;
            let shifted: Vec<_> = bm.bools().collect();
            let expected: Vec<_> = bools.iter().skip(bits).cloned().collect();
            assert_eq!(expected, shifted);
        }
        Ok(())
    }

    #[test]
    fn test_bitmap_extend1() -> Result<()> {
        rand_bitmap_extend(1, 1)
    }

    #[test]
    fn test_bitmap_extend2() -> Result<()> {
        rand_bitmap_extend(32, 33)
    }

    #[test]
    fn test_bitmap_extend3() -> Result<()> {
        rand_bitmap_extend(31, 35)
    }

    #[test]
    fn test_bitmap_extend4() -> Result<()> {
        rand_bitmap_extend(31, 75)
    }

    #[test]
    fn test_bitmap_extend5() -> Result<()> {
        rand_bitmap_extend(75, 31)
    }

    #[test]
    fn test_bitmap_extend6() -> Result<()> {
        rand_bitmap_extend(75, 75)
    }

    #[test]
    fn test_bitmap_extend7() -> Result<()> {
        rand_bitmap_extend(110, 129)
    }

    #[test]
    fn test_bitmap_extend8() -> Result<()> {
        rand_bitmap_extend(59, 197)
    }

    #[test]
    fn test_bitmap_extend9() -> Result<()> {
        rand_bitmap_extend(35, 64)
    }

    // generate random bitmaps and test shift operations
    #[test]
    fn test_bitmap_extend10() -> Result<()> {
        let mut rng = rand::thread_rng();
        for _ in 0..1024 {
            let size1: usize = rng.gen_range(0..256);
            let mut bools1 = Vec::with_capacity(size1);
            let size2: usize = rng.gen_range(0..256);
            let mut bools2 = Vec::with_capacity(size2);
            for _ in 0..size1 {
                bools1.push(rng.gen());
            }
            for _ in 0..size2 {
                bools2.push(rng.gen());
            }
            let expected: Vec<_> = bools1.iter().chain(bools2.iter()).cloned().collect();
            let mut bm1 = VecBitmap::from(bools1);
            let bm2 = VecBitmap::from(bools2);
            bm1.extend(&bm2)?;
            let actual: Vec<_> = bm1.bools().collect();
            assert_eq!(
                expected,
                actual,
                "bm1={}, bm2={}, bools={}",
                bm1.len(),
                bm2.len(),
                expected.len()
            );
        }
        Ok(())
    }

    #[test]
    fn test_bitmap_extend_const() -> Result<()> {
        let mut bm1 = VecBitmap::new();
        bm1.add(true)?;
        bm1.extend_const(false, 1)?;
        assert_eq!(vec![true, false], bm1.bools().collect::<Vec<_>>());
        bm1.extend_const(true, 10)?;
        assert_eq!(
            vec![true, false]
                .into_iter()
                .chain(vec![true; 10].into_iter())
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>()
        );
        bm1.extend_const(false, 65)?;
        assert_eq!(
            vec![true, false]
                .into_iter()
                .chain(std::iter::repeat(true).take(10))
                .chain(std::iter::repeat(false).take(65))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>()
        );
        bm1.clear();
        bm1.extend_const(true, 8)?;
        assert_eq!(vec![true; 8], bm1.bools().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn test_bitmap_extend_range1() -> Result<()> {
        let mut bm1 = VecBitmap::new();
        let bm2 = VecBitmap::from_iter(
            std::iter::repeat(true)
                .take(100)
                .chain(std::iter::repeat(false).take(200))
                .chain(std::iter::repeat(true).take(300)),
        );
        bm1.extend_range(&bm2, 0..5)?;
        assert_eq!(vec![true; 5], bm1.bools().collect::<Vec<_>>());
        bm1.extend_range(&bm2, 90..110)?;
        assert_eq!(
            std::iter::repeat(true)
                .take(15)
                .chain(std::iter::repeat(false).take(10))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>(),
        );
        bm1.extend_range(&bm2, 50..550)?;
        assert_eq!(525, bm1.len());
        assert_eq!(
            std::iter::repeat(true)
                .take(15)
                .chain(std::iter::repeat(false).take(10))
                .chain(std::iter::repeat(true).take(50))
                .chain(std::iter::repeat(false).take(200))
                .chain(std::iter::repeat(true).take(250))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>(),
        );
        Ok(())
    }

    #[test]
    fn test_bitmap_extend_range2() -> Result<()> {
        let mut bm1 = VecBitmap::from(vec![true, false, true]);
        let bm2 = VecBitmap::from(vec![true, false, true, true, false, true]);
        bm1.extend_range(&bm2, 3..5)?;
        assert_eq!(
            vec![true, false, true, true, false],
            bm1.bools().collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_bitmap_extend_range3() -> Result<()> {
        let mut bm1 = VecBitmap::from(vec![false; 40]);
        bm1.add(true)?;
        let bm2 = VecBitmap::from(vec![true; 64]);
        bm1.extend_range(&bm2, 10..15)?;
        assert_eq!(
            std::iter::repeat(false)
                .take(40)
                .chain(std::iter::repeat(true).take(6))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>(),
        );
        Ok(())
    }

    #[test]
    fn test_bitmap_from_iter() -> Result<()> {
        let bm1 = VecBitmap::from_iter(vec![true; 10]);
        assert_eq!(vec![true; 10], bm1.bools().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn test_bitmap_count1() {
        for i in vec![1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 1023, 1024, 1025] {
            rand_bitmap_count(i)
        }
    }

    #[test]
    fn test_bitmap_count2() {
        for _ in 0..256 {
            rand_bitmap_count(rand::thread_rng().gen_range(0..4096))
        }
    }

    #[test]
    fn test_bitmap_count3() {
        let bools1: Vec<bool> = vec![];
        let bm1 = VecBitmap::from(bools1);
        assert_eq!(0, bm1.true_count());
    }

    #[test]
    fn test_bitmap_range_iter1() -> Result<()> {
        for bs in vec![
            vec![true],
            vec![false],
            vec![true, false],
            vec![false, true],
            vec![true, false, true],
            vec![false, true, false],
            vec![true; 31],
            vec![false; 31],
            vec![true; 33],
            vec![false; 33],
            vec![true; 65],
            vec![false; 65],
            vec![true; 65].into_iter().chain(vec![false; 31]).collect(),
            vec![false; 65].into_iter().chain(vec![true; 63]).collect(),
            vec![false; 253]
                .into_iter()
                .chain(vec![true; 311])
                .chain(vec![false; 471])
                .collect(),
        ] {
            let expected = bs.len();
            let mut bm = VecBitmap::new();
            bm.extend_bools(bs)?;
            let mut actual = 0;
            for (_, n) in bm.range_iter() {
                actual += n;
            }
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    #[test]
    fn test_bitmap_range_iter2() {
        let mut rng = rand::thread_rng();
        for _ in 0..128 {
            let size: usize = rng.gen_range(128..4096);
            let mut bools: Vec<bool> = Vec::with_capacity(size);
            for _ in 0..size {
                bools.push(rng.gen());
            }
            let bm = VecBitmap::from(bools.clone());
            let mut base = 0;
            for (b, n) in bm.range_iter() {
                assert!(n != 0);
                for i in 0..n {
                    assert_eq!(b, bools[base + i]);
                }
                base += n;
            }
        }
    }

    #[test]
    fn test_bitmap_for_each1() {
        let bm = VecBitmap::from(vec![true, false, true, false, false, true, true]);
        let mut true_count = 0;
        bm.for_each(|b| {
            true_count += if b { 1 } else { 0 };
        });
        assert_eq!(4, true_count);

        let mut false_count = 0;
        bm.for_each_range(|b, n| {
            false_count += if b { 0 } else { n };
        });
        assert_eq!(3, false_count);

        for i in 64 - 8..64 + 8 {
            let bm = VecBitmap::from(vec![true; i]);
            true_count = 0;
            bm.for_each(|b| {
                true_count += if b { 1 } else { 0 };
            });
            assert_eq!(i, true_count);
            false_count = 0;
            bm.for_each_range(|b, n| {
                false_count += if b { 0 } else { n };
            });
            assert_eq!(0, false_count);

            let bm = VecBitmap::from(vec![false; i]);
            true_count = 0;
            bm.for_each(|b| {
                true_count += if b { 1 } else { 0 };
            });
            assert_eq!(0, true_count);
            false_count = 0;
            bm.for_each_range(|b, n| {
                false_count += if b { 0 } else { n };
            });
            assert_eq!(i, false_count);
        }
    }

    #[test]
    fn test_bitmap_for_each2() -> anyhow::Result<()> {
        let vs = vec![true, false, true, false, true, false, true];
        let mut bm = VecBitmap::with_capacity(64);
        for _ in 0..64 {
            bm.extend_bools(vs.iter().cloned())?;
        }
        let mut true_count = 0;
        bm.for_each(|b| {
            true_count += if b { 1 } else { 0 };
        });
        assert_eq!(4 * 64, true_count);
        let mut false_count = 0;
        bm.for_each_range(|b, n| {
            false_count += if b { 0 } else { n };
        });
        assert_eq!(3 * 64, false_count);
        let mut total_count = 0;
        for (_, n) in bm.range_iter() {
            total_count += n;
        }
        assert_eq!(7 * 64, total_count);
        Ok(())
    }

    #[test]
    fn test_bitmap_for_each3() -> anyhow::Result<()> {
        let mut bm = VecBitmap::with_capacity(64);
        for _ in 0..8 {
            bm.extend_bools(vec![true; 64])?;
            bm.extend_bools(vec![false; 64])?;
        }
        let mut true_count = 0;
        bm.for_each(|b| {
            true_count += if b { 1 } else { 0 };
        });
        assert_eq!(8 * 64, true_count);
        let mut false_count = 0;
        bm.for_each_range(|b, n| {
            false_count += if b { 0 } else { n };
        });
        assert_eq!(8 * 64, false_count);
        let mut total_count = 0;
        for (_, n) in bm.range_iter() {
            total_count += n;
        }
        assert_eq!(16 * 64, total_count);
        Ok(())
    }

    #[test]
    fn test_bitmap_for_each4() -> anyhow::Result<()> {
        let mut bm = VecBitmap::new();
        bm.extend_bools(vec![true; 64])?;
        bm.add(false)?;
        bm.add(true)?;
        bm.add(false)?;
        let mut false_count = 0;
        bm.for_each_range(|b, n| {
            false_count += if b { 0 } else { n };
        });
        assert_eq!(2, false_count);
        false_count = 0;
        for (b, n) in bm.range_iter() {
            false_count += if b { 0 } else { n };
        }
        assert_eq!(2, false_count);

        bm.clear();
        bm.extend_bools(vec![false; 64])?;
        bm.add(true)?;
        bm.add(false)?;
        bm.add(true)?;
        let mut true_count = 0;
        bm.for_each_range(|b, n| {
            true_count += if b { n } else { 0 };
        });
        assert_eq!(2, true_count);
        true_count = 0;
        for (b, n) in bm.range_iter() {
            true_count += if b { n } else { 0 };
        }
        assert_eq!(2, true_count);

        Ok(())
    }

    #[test]
    fn test_bitmap_for_each5() -> anyhow::Result<()> {
        for i in 0..32 {
            let bm = VecBitmap::from(vec![true; i]);
            let mut sum = 0;
            bm.for_each(|_| {
                sum += 1;
            });
            assert_eq!(i, sum);
        }
        Ok(())
    }

    #[test]
    fn test_bitmap_empty_range_iter() {
        let bm = VecBitmap::with_capacity(64);
        let mut count = 0;
        for (_, n) in bm.range_iter() {
            count += n;
        }
        assert_eq!(0, count);
    }

    #[test]
    fn test_bitmap_merge() {
        let mut bm0 = VecBitmap::from(vec![true, false, true, false]);
        let bm1 = VecBitmap::from(vec![false, false, true]);
        assert!(bm0.merge(&bm1).is_err());
        let bm2 = VecBitmap::from(vec![false, false, true, true]);
        bm0.merge(&bm2).unwrap();
        assert_eq!(
            vec![false, false, true, false],
            bm0.bools().collect::<Vec<_>>()
        );
    }

    fn rand_bitmap_count(n: usize) {
        let mut rng = rand::thread_rng();
        let mut bools1: Vec<bool> = Vec::with_capacity(n);
        for _ in 0..n {
            bools1.push(rng.gen());
        }
        let expected_true: usize = bools1.iter().map(|b| if *b { 1 } else { 0 }).sum();
        let expected_false: usize = bools1.iter().map(|b| if *b { 0 } else { 1 }).sum();
        let bm1 = VecBitmap::from(bools1);
        let actual_true = bm1.true_count();
        let actual_false = bm1.false_count();
        assert_eq!(expected_true, actual_true);
        assert_eq!(expected_false, actual_false);
    }

    fn rand_bitmap_extend(n1: usize, n2: usize) -> Result<()> {
        let mut rng = rand::thread_rng();
        let mut bools1: Vec<bool> = Vec::with_capacity(n1);
        for _ in 0..n1 {
            bools1.push(rng.gen());
        }
        let mut bools2: Vec<bool> = Vec::with_capacity(n2);
        for _ in 0..n2 {
            bools2.push(rng.gen());
        }
        let expected: Vec<_> = bools1.iter().chain(bools2.iter()).cloned().collect();
        let mut bm1 = VecBitmap::from(bools1);
        let bm2 = VecBitmap::from(bools2);
        bm1.extend(&bm2)?;
        let actual: Vec<_> = bm1.bools().collect();
        assert_eq!(expected, actual);
        Ok(())
    }
}
