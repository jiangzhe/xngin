mod vec;
mod view;

use crate::error::{Error, Result};
use crate::slice_ext::{OffsetPairMut, OffsetTripleMut, PairSliceExt};
use std::ops::Range;

pub use vec::VecBitmap;
pub use view::ViewBitmap;

pub trait ReadBitmap {
    /// Returns aligned(64bit) bitmap and length.
    /// This method is marked as unsafe because hidden bits outside
    /// the map could be seen from the returned slice.
    /// In most cases, it is used for fast aligned operations, like
    /// merge, shift and extend.
    fn aligned(&self) -> (&[u8], usize);

    /// Returns the length of this map.
    fn len(&self) -> usize;

    /// Returns whether this map is empty.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get single value at given position.
    #[inline]
    fn get(&self, idx: usize) -> Result<bool> {
        let (bm, len) = self.aligned();
        if idx >= len {
            return Err(Error::IndexOutOfBound(format!(
                "{} > bitmap length {}",
                idx, len
            )));
        }
        Ok(bitmap_get(bm, idx))
    }

    #[inline]
    fn bools(&self) -> BoolIter<'_> {
        let (bm, len) = self.aligned();
        bitmap_bools(bm, len)
    }

    /// Returns value count of true.
    #[inline]
    fn true_count(&self) -> usize {
        let (bm, len) = self.aligned();
        bitmap_true_count(bm, len)
    }

    /// Returns value count of false.
    /// bytemuck to speed up, u32 and u128 are also tested but benchmark
    /// shows u64 is best.
    #[inline]
    fn false_count(&self) -> usize {
        let (bm, len) = self.aligned();
        bitmap_false_count(bm, len)
    }

    #[inline]
    fn range_iter(&self) -> RangeIter<'_> {
        let (bm, len) = self.aligned();
        bitmap_range_iter(bm, len)
    }
}

#[inline]
pub fn bitmap_get(bm: &[u8], idx: usize) -> bool {
    bm[idx >> 3] & (1 << (idx & 7)) != 0
}

#[inline]
pub fn bitmap_bools(bm: &[u8], len: usize) -> BoolIter<'_> {
    BoolIter { bm, len, idx: 0 }
}

#[inline]
pub fn bitmap_true_count(bm: &[u8], len: usize) -> usize {
    let mut bytes = len >> 3;
    let u64bytes = (bytes >> 3) << 3;
    bytes -= u64bytes;
    let bits = len & 7;
    // sum of u64s
    let mut sum0: usize = bytemuck::cast_slice::<_, u64>(&bm[..u64bytes])
        .iter()
        .map(|v| v.count_ones() as usize)
        .sum();
    // sum of bytes
    if bytes > 0 {
        sum0 += bm[u64bytes..u64bytes + bytes]
            .iter()
            .map(|v| v.count_ones() as usize)
            .sum::<usize>();
    }
    if bits > 0 {
        // mask higher bits before counting
        sum0 += (bm[u64bytes + bytes] & ((1 << bits) - 1)).count_ones() as usize;
    }
    sum0
}

#[inline]
pub fn bitmap_false_count(bm: &[u8], len: usize) -> usize {
    let mut bytes = len >> 3;
    let u64bytes = (bytes >> 3) << 3;
    bytes -= u64bytes;
    let bits = len & 7;
    // sum of u64s
    let mut sum0: usize = bytemuck::cast_slice::<_, u64>(&bm[..u64bytes])
        .iter()
        .map(|v| v.count_zeros() as usize)
        .sum();
    // sum of bytes
    if bytes > 0 {
        sum0 += bm[u64bytes..u64bytes + bytes]
            .iter()
            .map(|v| v.count_zeros() as usize)
            .sum::<usize>();
    }
    if bits > 0 {
        // mask higher bits before counting
        sum0 += (bm[u64bytes + bytes] | !((1 << bits) - 1)).count_zeros() as usize;
    }
    sum0
}

#[inline]
pub fn bitmap_range_iter(bm: &[u8], len: usize) -> RangeIter<'_> {
    // validate given bitmap is aligned per 8 bytes.
    assert!(bm.len() & 7 == 0);
    if len == 0 {
        // empty iterator
        return RangeIter {
            u64s: &[],
            last_word_len: 0,
            word: 0,
            word_bits: 0,
            prev: false,
            n: 0,
        };
    }
    let prev = bm[0] & 1 != 1; // pre-read first value
    let u64s = bytemuck::cast_slice::<_, u64>(bm);
    let last_word_len = if len & 63 == 0 { 64 } else { len & 63 };
    RangeIter {
        u64s,
        last_word_len,
        word: 0,
        word_bits: 0,
        prev,
        n: 0,
    }
}

#[inline]
pub fn bitmap_first_true(bm: &[u8], len: usize) -> Option<usize> {
    let mut idx = 0;
    for (f, n) in bitmap_range_iter(bm, len) {
        if f {
            return Some(idx);
        } else {
            idx += n;
        }
    }
    None
}

#[inline]
pub fn bitmap_merge(this: &mut [u8], this_len: usize, that: &[u8], that_len: usize) {
    assert!(this_len == that_len);
    this.iter_mut().zip(that.iter()).for_each(|(a, b)| *a &= b);
}

#[inline]
pub fn bitmap_for_each<F: FnMut(bool)>(bm: &[u8], len: usize, mut f: F) {
    if len == 0 {
        return;
    }
    bm[..len >> 3].iter().for_each(|b| {
        f(b & 1 == 1);
        f(b & 2 == 2);
        f(b & 4 == 4);
        f(b & 8 == 8);
        f(b & 16 == 16);
        f(b & 32 == 32);
        f(b & 64 == 64);
        f(b & 128 == 128);
    });
    if len & 7 == 0 {
        return;
    }
    let b = bm[len >> 3];
    let idx = len & 7;
    f(b & 1 == 1);
    if idx == 1 {
        return;
    }
    f(b & 2 == 2);
    if idx == 2 {
        return;
    }
    f(b & 4 == 4);
    if idx == 3 {
        return;
    }
    f(b & 8 == 8);
    if idx == 4 {
        return;
    }
    f(b & 16 == 16);
    if idx == 5 {
        return;
    }
    f(b & 32 == 32);
    if idx == 6 {
        return;
    }
    f(b & 64 == 64);
}

#[inline]
pub fn bitmap_for_each_range<F: FnMut(bool, usize)>(bm: &[u8], len: usize, mut f: F) {
    if len == 0 {
        return;
    }
    let mut prev = bm[0] & 1 == 1; // pre-read first value
    let mut n: usize = 0;
    let traverse_u64s = |i: &u64| {
        let mut i = *i;
        match i {
            0 => {
                // all falses
                if !prev {
                    n += 64;
                } else {
                    f(prev, n);
                    prev = false;
                    n = 64;
                }
            }
            0xffff_ffff_ffff_ffff => {
                // all trues
                if prev {
                    n += 64;
                } else {
                    f(prev, n);
                    prev = true;
                    n = 64;
                }
            }
            _ => {
                let mut word_bits: usize = 64;
                if prev {
                    let true_bits = i.trailing_ones() as usize;
                    if true_bits > 0 {
                        n += true_bits;
                        i >>= true_bits;
                        word_bits -= true_bits;
                    }
                } else {
                    let false_bits = i.trailing_zeros() as usize;
                    if false_bits > 0 {
                        n += false_bits;
                        i >>= false_bits;
                        word_bits -= false_bits;
                    }
                }
                while word_bits > 0 {
                    f(prev, n);
                    if prev {
                        prev = false;
                        n = word_bits.min(i.trailing_zeros() as usize);
                    } else {
                        prev = true;
                        n = word_bits.min(i.trailing_ones() as usize);
                    }
                    i >>= n;
                    word_bits -= n;
                }
            }
        }
    };

    if len & 63 == 0 {
        let u64s = bytemuck::cast_slice::<_, u64>(bm);
        u64s.iter().for_each(traverse_u64s);
        f(prev, n);
        return;
    }
    let mut last_word_len = len & 63;
    let u64s = bytemuck::cast_slice::<_, u64>(bm);
    let (last, head) = u64s.split_last().unwrap();
    head.iter().for_each(traverse_u64s);
    // handle last word
    let mut last = *last;
    if prev {
        let true_bits = last.trailing_ones() as usize;
        if true_bits >= last_word_len {
            f(prev, n + last_word_len);
            return;
        } else if true_bits > 0 {
            n += true_bits;
            last >>= true_bits;
            last_word_len -= true_bits;
        }
    } else {
        let false_bits = last.trailing_zeros() as usize;
        if false_bits >= last_word_len {
            f(prev, n + last_word_len);
            return;
        } else if false_bits > 0 {
            n += false_bits;
            last >>= false_bits;
            last_word_len -= false_bits;
        }
    }
    loop {
        if last_word_len == 0 {
            f(prev, n);
            return;
        }
        f(prev, n);
        prev = !prev; // flip the flag
        if prev {
            n = last_word_len.min(last.trailing_ones() as usize);
        } else {
            n = last_word_len.min(last.trailing_zeros() as usize);
        }
        last >>= n;
        last_word_len -= n;
    }
}

#[derive(Debug, Clone)]
pub struct RangeIter<'a> {
    u64s: &'a [u64],      // slice of u64
    last_word_len: usize, // length of last word
    word: u64,            // current u64 word to scan
    word_bits: usize,     // maximum bits in current word
    prev: bool,           // previous value (true/flase)
    n: usize,             // previous repeat number
}

impl<'a> RangeIter<'a> {
    #[inline]
    fn break_falses_in_word(&mut self) {
        debug_assert!(self.prev);
        let bits = self.word_bits.min(self.word.trailing_zeros() as usize);
        if bits == 64 {
            self.word = 0;
        } else {
            self.word >>= bits;
        }
        self.prev = false;
        self.n = bits;
        self.word_bits -= bits;
    }

    #[inline]
    fn continue_falses_in_word(&mut self) {
        debug_assert!(!self.prev);
        let bits = self.word_bits.min(self.word.trailing_zeros() as usize);
        self.word >>= bits;
        self.word_bits -= bits;
        self.n += bits;
    }

    #[inline]
    fn break_trues_in_word(&mut self) {
        debug_assert!(!self.prev);
        let bits = self.word_bits.min(self.word.trailing_ones() as usize);
        if bits == 64 {
            self.word = 0;
        } else {
            self.word >>= bits;
        }
        self.prev = true;
        self.n = bits;
        self.word_bits -= bits;
    }

    #[inline]
    fn continue_trues_in_word(&mut self) {
        debug_assert!(self.prev);
        let bits = self.word_bits.min(self.word.trailing_ones() as usize);
        self.word >>= bits;
        self.word_bits -= bits;
        self.n += bits;
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = (bool, usize);
    /// Returns bool value with its repeat number.
    /// The implementation scans the bitmap on two levels.
    /// u64 word level and bit level.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.word_bits == 0 {
            'INIT_WORD: loop {
                match self.u64s.len() {
                    0 => {
                        if self.n == 0 {
                            // iterator exhausted
                            return None;
                        } else {
                            // output last item
                            let rg = (self.prev, self.n);
                            self.n = 0;
                            return Some(rg);
                        }
                    }
                    1 => {
                        // prepare last word
                        self.word = self.u64s[0];
                        self.word_bits = self.last_word_len;
                        self.u64s = &[];
                        if self.prev {
                            self.continue_trues_in_word();
                            if self.n == 0 {
                                self.prev = !self.prev;
                                self.continue_falses_in_word();
                            }
                        } else {
                            self.continue_falses_in_word();
                            if self.n == 0 {
                                self.prev = !self.prev;
                                self.continue_trues_in_word();
                            }
                        }
                        break 'INIT_WORD;
                    }
                    _ => {
                        // fast-scan current word
                        self.word = self.u64s[0];
                        self.u64s = &self.u64s[1..];
                        match self.word {
                            0 => {
                                if self.prev {
                                    // all falses and prev is true
                                    let rg = (self.prev, self.n);
                                    self.prev = false;
                                    self.n = 64;
                                    return Some(rg);
                                } else {
                                    // all falses and prev is also false
                                    self.n += 64;
                                }
                            }
                            0xffff_ffff_ffff_ffff => {
                                if !self.prev {
                                    // all trues and prev is false
                                    let rg = (self.prev, self.n);
                                    self.prev = true;
                                    self.n = 64;
                                    return Some(rg);
                                } else {
                                    // all trues and prev is also true
                                    self.n += 64;
                                }
                            }
                            _ => {
                                self.word_bits = 64;
                                if self.prev {
                                    self.continue_trues_in_word();
                                    if self.n == 0 {
                                        self.prev = !self.prev;
                                        self.continue_falses_in_word();
                                    }
                                } else {
                                    self.continue_falses_in_word();
                                    if self.n == 0 {
                                        self.prev = !self.prev;
                                        self.continue_trues_in_word();
                                    }
                                }
                                break 'INIT_WORD;
                            }
                        }
                    }
                }
            }
        }
        let ret = (self.prev, self.n);
        if self.prev {
            self.break_falses_in_word();
        } else {
            self.break_trues_in_word();
        }
        Some(ret)
    }
}

pub trait ReadBitmapExt: ReadBitmap {
    fn for_each<F: FnMut(bool)>(&self, f: F) {
        let (bm, len) = self.aligned();
        bitmap_for_each(bm, len, f)
    }

    fn for_each_range<F: FnMut(bool, usize)>(&self, f: F) {
        let (bm, len) = self.aligned();
        bitmap_for_each_range(bm, len, f)
    }
}

impl<T: ReadBitmap> ReadBitmapExt for T {}

pub trait WriteBitmap: ReadBitmap {
    /// Returns mutable ref to a bitmap.
    fn aligned_mut(&mut self) -> (&mut [u8], usize);

    /// Clear this map.
    fn clear(&mut self);

    /// Set length of this map.
    /// This method is NOT marked as unsafe because it reports error
    /// of length is greater than capacity.
    /// The implementation MUST check the length before applying the update.
    fn set_len(&mut self, len: usize) -> Result<()>;

    /// Set single value to the bitmap at given position.
    #[inline]
    fn set(&mut self, idx: usize, val: bool) -> Result<()> {
        let (bm, len) = self.aligned_mut();
        if idx >= len {
            return Err(Error::IndexOutOfBound(format!(
                "{} >= bitmap length {}",
                idx, len
            )));
        }
        bitmap_set(bm, idx, val);
        Ok(())
    }

    /// Shift bitmap to left with give length.
    #[inline]
    fn shift(&mut self, bits: usize) -> Result<()> {
        let (bm, len) = self.aligned_mut();
        bitmap_shift(bm, len, bits);
        self.set_len(len - bits)
    }

    /// Merges given bitmap to current one.
    /// Compiler will apply SIMD optimization automatically, so u64 bytemuck
    /// is unnecessary.
    #[inline]
    fn merge<T>(&mut self, that: &T) -> Result<()>
    where
        T: ReadBitmap,
    {
        let (this, this_len) = self.aligned_mut();
        let (that, that_len) = that.aligned();
        if this_len != that_len {
            return Err(Error::InvalidArgument(format!(
                "lengths of merging bitmaps mismatch {} != {}",
                this_len, that_len
            )));
        }
        bitmap_merge(this, this_len, that, that_len);
        Ok(())
    }

    /// Inverse all values in this bitmap
    #[inline]
    fn inverse(&mut self) {
        let (bm, _) = self.aligned_mut();
        bm.iter_mut().for_each(|x| *x = !*x)
    }
}

#[inline]
pub fn bitmap_set(bm: &mut [u8], idx: usize, val: bool) {
    let bidx = idx >> 3;
    if val {
        bm[bidx] |= 1 << (idx & 7);
    } else {
        bm[bidx] &= !(1 << (idx & 7));
    }
}

pub trait AppendBitmap: WriteBitmap {
    /// Add given flag to end of bitmap.
    fn add(&mut self, val: bool) -> Result<()>;

    /// Extend this bitmap with another one.
    /// The method is very fast if original length is multiply of 8.
    /// Otherwise, we need to do bit shifting for each byte.
    /// Although the performance is tuned, it's still much slower
    /// (4-5x slower) compared to the one of 8x length.
    fn extend<T: ReadBitmap + ?Sized>(&mut self, other: &T) -> Result<()>;

    /// Extend this bitmap with another one of given range.
    fn extend_range<T: ReadBitmap + ?Sized>(
        &mut self,
        other: &T,
        range: Range<usize>,
    ) -> Result<()>;

    /// Extend this bitmap with constant bool value.
    fn extend_const(&mut self, val: bool, len: usize) -> Result<()>;

    /// Extend this bitmap with bool values.
    fn extend_bools<T>(&mut self, bools: T) -> Result<()>
    where
        T: IntoIterator<Item = bool>,
        Self: Sized,
    {
        let orig_len = self.len();
        let eb = ExtendBools {
            bm: self,
            orig_len,
            success: false,
        };
        eb.run(bools)
    }
}

// input bs must have length of multiple of 8.
#[inline]
fn bitmap_shift(bs: &mut [u8], len: usize, mut bits: usize) {
    if bits >= len || bits == 0 {
        return;
    }
    if bits & 7 == 0 {
        // move bytes
        bs.copy_within((bits >> 3).., 0);
        return;
    }
    // shift as u64
    let orig_u64s = bs.len() >> 3;
    let offset_u64s = bits >> 6;
    let u64bs = bytemuck::cast_slice_mut::<_, u64>(&mut bs[..orig_u64s * 8]);
    bits &= 63;
    if offset_u64s == 0 {
        let rbits = 64 - bits;
        u64bs.for_each_offset_pair(1, |(a, b)| {
            *a >>= bits;
            *a |= *b << rbits;
        });
        *u64bs.last_mut().unwrap() >>= bits;
    } else {
        // Use macro to unroll below expression for better performance.
        //
        // u64bs.for_each_offset_triple(offset_u64s, offset_u64s+1, |(a, b, c)| {
        //     *a = (*b >> bits) | (*c << rbits);
        // });
        macro_rules! shift_with_triple {
            ($bits:literal) => {
                u64bs.for_each_offset_triple(
                    offset_u64s,
                    offset_u64s + 1,
                    |(a, b, c): (&mut u64, &u64, &u64)| {
                        *a = (*b >> $bits) | (*c << (64 - $bits));
                    },
                )
            };
        }
        macro_rules! shift_branched_triples {
            ($bits:expr, $($branch:literal),+) => {
                match $bits {
                    $(
                        $branch => shift_with_triple!($branch),
                    )+
                    _ => unreachable!(),
                }
            }
        }
        shift_branched_triples!(
            bits, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
            45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63
        );
        u64bs[u64bs.len() - offset_u64s - 1] = u64bs[u64bs.len() - 1] >> bits;
    }
}

// dst and src must be 64-bit aligned.
#[inline]
fn copy_bits(dst: &mut [u8], dst_len: usize, src: &[u8], src_len: usize) {
    debug_assert!(src.len() * 8 >= src_len);
    debug_assert!(dst.len() * 8 >= dst_len + src_len);
    debug_assert!(dst.len() & 7 == 0); // always multiply of 8
    debug_assert!(src.len() & 7 == 0); // always multiply of 8
    if src_len == 0 {
        // nothing to do
        return;
    }
    if dst_len & 7 == 0 {
        // copy bytes
        let src_bytes = (src_len + 7) / 8;
        let dst_bytes = dst_len / 8;
        dst[dst_bytes..dst_bytes + src_bytes].copy_from_slice(&src[..src_bytes]);
        return;
    }
    let rbits = dst_len & 63;
    let bits = 64 - rbits;
    let tgt_len = dst_len + src_len;
    let tgt_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
    let src_u64s_bytes = ((src_len + 63) >> 6) << 3;
    let orig_dst_u64s_bytes = (dst_len >> 6) << 3; // last u64 in original bitmap is treated as new.
    let u64dst = bytemuck::cast_slice_mut::<_, u64>(&mut dst[orig_dst_u64s_bytes..tgt_u64s_bytes]);
    let u64src = bytemuck::cast_slice::<_, u64>(&src[..src_u64s_bytes]);
    // update first u64 of destination
    let first_u64 = &mut u64dst[0];
    *first_u64 &= (1 << rbits) - 1;
    *first_u64 |= u64src[0] << rbits;
    // update with src pairs
    // benchmark shows unrolling the bits literal with macros decrease
    // performance a bit(15~20%), so keep it as is.
    u64dst[1..]
        .iter_mut()
        .zip(u64src.pairs())
        .for_each(|(a, (b, c))| {
            *a = *b >> bits;
            *a |= *c << rbits;
        });
    if src_len & 63 > bits || src_len & 63 == 0 {
        // addtional u64 to store the rest of bits
        *u64dst.last_mut().unwrap() = *u64src.last().unwrap() >> bits;
    }
}

#[inline]
fn copy_bits_range(dst: &mut [u8], dst_len: usize, src: &[u8], range: Range<usize>) {
    debug_assert!(src.len() * 8 >= range.end);
    let tgt_len = dst_len + range.end - range.start;
    debug_assert!(dst.len() * 8 >= tgt_len);
    debug_assert!(dst.len() & 7 == 0); // always multiply of 8
    debug_assert!(src.len() & 7 == 0); // always multiply of 8
    if range.start & 7 == 0 {
        // start aligned to byte bound, reuse copy_bits
        copy_bits(
            dst,
            dst_len,
            &src[range.start / 8..],
            range.end - range.start,
        );
        return;
    }
    if dst_len & 7 == range.start & 7 {
        // last few dst bits and first src bits just compose one byte
        let rbits = dst_len & 7;
        let dst_last_idx = dst_len / 8;
        let dst_last_byte = &mut dst[dst_last_idx];
        *dst_last_byte &= (1 << rbits) - 1;
        let src_first_byte = src[range.start / 8];
        *dst_last_byte |= src_first_byte & !((1 << rbits) - 1);
        let src_start = range.start / 8;
        let src_end = (range.end + 7) / 8;
        dst[dst_last_idx + 1..dst_last_idx + src_end - src_start]
            .copy_from_slice(&src[src_start + 1..src_end]);
        return;
    }
    // try u64
    let rbits = dst_len & 63;
    let src_start_bits = range.start & 63;
    if rbits < src_start_bits {
        // shift right to fill dst
        let shr_bits = src_start_bits - rbits;
        let shl_bits = 64 - shr_bits;
        let orig_dst_u64s_bytes = (dst_len >> 6) << 3;
        let tgt_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
        let u64dst =
            bytemuck::cast_slice_mut::<_, u64>(&mut dst[orig_dst_u64s_bytes..tgt_u64s_bytes]);
        let src_u64s_start = (range.start >> 6) << 3;
        let src_u64s_end = ((range.end + 63) >> 6) << 3;
        let u64src = bytemuck::cast_slice::<_, u64>(&src[src_u64s_start..src_u64s_end]);
        // save first u64, handling it after pair updates
        let u64dst_first = u64dst[0] & ((1 << rbits) - 1);
        // pair update
        u64dst
            .iter_mut()
            .zip(u64src.pairs())
            .for_each(|(a, (b, c))| {
                *a = (*b >> shr_bits) | (*c << shl_bits);
            });
        // update first u64
        u64dst[0] &= !((1 << rbits) - 1); // clear original bits
        u64dst[0] |= u64dst_first; // update original value back
                                   // update last u64
        if (range.end & 63) > shr_bits {
            // additional u64 to update last few bits from src
            *u64dst.last_mut().unwrap() = *u64src.last().unwrap() >> shr_bits;
        }
    } else {
        // shift left to fill dst
        let shl_bits = rbits - src_start_bits;
        let shr_bits = 64 - shl_bits;
        let orig_dst_u64s_bytes = (dst_len >> 6) << 3;
        let tgt_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
        let u64dst =
            bytemuck::cast_slice_mut::<_, u64>(&mut dst[orig_dst_u64s_bytes..tgt_u64s_bytes]);
        let src_u64s_start = (range.start >> 6) << 3;
        let src_u64s_end = ((range.end + 63) >> 6) << 3;
        let u64src = bytemuck::cast_slice::<_, u64>(&src[src_u64s_start..src_u64s_end]);
        // update first u64
        u64dst[0] &= (1 << rbits) - 1;
        u64dst[0] |= (u64src[0] << shl_bits) & !((1 << rbits) - 1);
        u64dst[1..]
            .iter_mut()
            .zip(u64src.pairs())
            .for_each(|(a, (b, c))| {
                *a = b >> shr_bits;
                *a |= c << shl_bits;
            });
        // update last u64
        if (range.end & 63) > shr_bits {
            // additional u64 to update last few bits from src
            *u64dst.last_mut().unwrap() = *u64src.last().unwrap() >> shr_bits;
        }
    }
}

#[inline]
fn copy_const_bits(dst: &mut [u8], dst_len: usize, src_val: bool, src_len: usize) {
    debug_assert!(dst.len() * 8 >= dst_len + src_len);
    debug_assert!(dst.len() & 7 == 0); // always multiply of 8
    if src_len == 0 {
        // nothing to do
        return;
    }
    if dst_len & 7 == 0 {
        // copy bytes
        let byte = if src_val { 0xff } else { 0x00 };
        let src_bytes = (src_len + 7) / 8;
        let dst_bytes = dst_len / 8;
        dst[dst_bytes..dst_bytes + src_bytes]
            .iter_mut()
            .for_each(|b| *b = byte);
        return;
    }
    let rbits = dst_len & 63;
    let tgt_len = dst_len + src_len;
    let tgt_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
    let orig_dst_u64s_bytes = (dst_len >> 6) << 3; // last u64 in original bitmap is treated as new.
    let u64dst = bytemuck::cast_slice_mut::<_, u64>(&mut dst[orig_dst_u64s_bytes..tgt_u64s_bytes]);
    // update first u64 of destination
    let first_u64 = &mut u64dst[0];
    if src_val {
        *first_u64 |= !((1 << rbits) - 1);
    } else {
        *first_u64 &= (1 << rbits) - 1;
    }
    // update all other u64s
    let u64word = if src_val { 0xffff_ffff_ffff_ffff } else { 0 };
    u64dst[1..].iter_mut().for_each(|w| *w = u64word);
}

#[derive(Debug, Clone)]
pub struct BoolIter<'a> {
    bm: &'a [u8],
    len: usize,
    idx: usize,
}

impl<'a> Iterator for BoolIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.len {
            None
        } else {
            let b = self.bm[self.idx >> 3] & (1 << (self.idx & 7)) != 0;
            self.idx += 1;
            Some(b)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.len - self.idx))
    }
}

struct ExtendBools<'a, T: AppendBitmap> {
    bm: &'a mut T,
    orig_len: usize,
    success: bool,
}

impl<'a, T: AppendBitmap> Drop for ExtendBools<'a, T> {
    fn drop(&mut self) {
        if !self.success {
            self.bm.set_len(self.orig_len).unwrap(); // won't fail
        }
    }
}

impl<'a, T: AppendBitmap> ExtendBools<'a, T> {
    #[inline]
    fn run<I>(mut self, bools: I) -> Result<()>
    where
        I: IntoIterator<Item = bool>,
    {
        for b in bools {
            self.bm.add(b)?;
        }
        self.success = true;
        Ok(())
    }
}
