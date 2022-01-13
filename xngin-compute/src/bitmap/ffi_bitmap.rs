use crate::bitmap::{AppendBitmap, ReadBitmap, WriteBitmap};
use crate::error::{Error, Result};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct FFIBitmap {
    pub(super) ptr: *const u8,
    pub(super) bits: usize,
    pub(super) max_bits: usize,
}

impl FFIBitmap {
    /// Construct a FFIBitmap using raw pointer, with length and capacity.
    ///
    /// # Safety
    ///
    /// The caller must make sure the pointer is pointing to valid memory
    /// allocated by Rust allocator, and synchronize conccurent access.
    #[inline]
    pub unsafe fn new(ptr: *const u8, bits: usize, max_bits: usize) -> Result<Self> {
        if max_bits & 63 != 0 {
            return Err(Error::InvalidArgument(format!(
                "Capacity({}) of FFIBitmap is not multiply of 64",
                max_bits
            )));
        }
        if bits > max_bits {
            return Err(Error::InvalidArgument(format!(
                "Length of FFIBitmap({}) must be less than capacity({})",
                bits, max_bits
            )));
        }
        Ok(FFIBitmap {
            ptr,
            bits,
            max_bits,
        })
    }

    #[inline]
    unsafe fn cap_mut(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.max_bits >> 3)
    }
}

impl ReadBitmap for FFIBitmap {
    #[inline]
    fn as_bitmap(&self) -> (&[u8], usize) {
        let slice = unsafe { std::slice::from_raw_parts(self.ptr, (self.bits + 7) >> 3) };
        (slice, self.bits)
    }

    #[inline]
    fn cap(&self) -> usize {
        self.max_bits
    }

    #[inline]
    unsafe fn as_aligned_bitmap(&self) -> Result<(&[u8], usize)> {
        let aligned_bytes = ((self.bits + 63) >> 6) << 3;
        if aligned_bytes * 8 > self.max_bits {
            return Err(Error::InternalError(
                "Not sufficient space for aligned bitmap view".into(),
            ));
        }
        let bm = std::slice::from_raw_parts(self.ptr, aligned_bytes);
        Ok((bm, self.bits))
    }
}

impl WriteBitmap for FFIBitmap {
    #[inline]
    fn as_bitmap_mut(&mut self) -> (&mut [u8], usize) {
        let slice =
            unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, (self.bits + 7) >> 3) };
        (slice, self.bits)
    }

    #[inline]
    fn clear(&mut self) {
        self.bits = 0;
    }

    #[inline]
    fn set_len(&mut self, len: usize) -> Result<()> {
        if len <= self.bits || len <= self.max_bits {
            self.bits = len;
            Ok(())
        } else {
            Err(Error::IndexOutOfBound(format!(
                "set bitmap length greater than capacity {} > {}",
                len, self.max_bits
            )))
        }
    }
}

impl AppendBitmap for FFIBitmap {
    #[inline]
    fn add(&mut self, val: bool) -> Result<()> {
        if self.bits >= self.max_bits {
            return Err(Error::InternalError("FFIBitmap is full".into()));
        }
        let idx = self.bits;
        let bm = unsafe { self.cap_mut() };
        if val {
            bm[idx >> 3] |= 1 << (idx & 7);
        } else {
            bm[idx >> 3] &= !(1 << (idx & 7));
        }
        self.bits += 1;
        Ok(())
    }

    #[inline]
    fn extend<T: ReadBitmap + ?Sized>(&mut self, other: &T) -> Result<()> {
        let (obm, olen) = unsafe { other.as_aligned_bitmap()? };
        let orig_len = self.bits;
        let tgt_len = orig_len + olen;
        let new_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
        if self.max_bits < (new_u64s_bytes << 3) {
            return Err(Error::InternalError(
                "Not sufficient capacity for FFIBitmap to extend".into(),
            ));
        }
        // SAFETY:
        // This is safe because we already check the capacity is no less than length of u64 slice.
        let bm = unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, new_u64s_bytes) };
        super::copy_bits(bm, orig_len, obm, olen);
        self.set_len(tgt_len)
    }

    #[inline]
    fn extend_range<T: ReadBitmap + ?Sized>(
        &mut self,
        that: &T,
        range: Range<usize>,
    ) -> Result<()> {
        let (tbm, tlen) = unsafe { that.as_aligned_bitmap()? };
        if range.end > tlen {
            return Err(Error::IndexOutOfBound(format!(
                "extend range {:?} larger than column length {}",
                range, tlen
            )));
        }
        let range_len = range.end - range.start;
        let orig_len = self.len();
        let tgt_len = orig_len + range_len;
        if tgt_len > self.max_bits {
            return Err(Error::InvalidArgument(
                "Not sufficient capacity for FFIBitmap to extend".into(),
            ));
        }
        let new_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
        // SAFETY:
        // capacity is checked before casting.
        let slice = unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, new_u64s_bytes) };
        super::copy_bits_range(slice, orig_len, tbm, range);
        self.set_len(tgt_len)
    }

    #[inline]
    fn extend_const(&mut self, val: bool, len: usize) -> Result<()> {
        let orig_len = self.bits;
        let tgt_len = orig_len + len;
        let new_u64s_bytes = ((tgt_len + 63) >> 6) << 3;
        if self.max_bits < (new_u64s_bytes << 3) {
            return Err(Error::InternalError(
                "Not sufficient capacity for FFIBitmap to extend".into(),
            ));
        }
        // SAFETY:
        // This is safe because we already check the capacity is no less than length of u64 slice.
        let bm = unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, new_u64s_bytes) };
        if val {
            super::copy_const_bits(bm, orig_len, true, len);
        } else {
            super::copy_const_bits(bm, orig_len, false, len);
        }
        self.set_len(tgt_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_bitmap_create_assertion() {
        unsafe {
            let raw_vec = vec![0u8; 8];
            let ptr = raw_vec.as_ptr();
            let res = FFIBitmap::new(ptr, 63, 63);
            assert!(res.is_err());
            let res = FFIBitmap::new(ptr, 128, 64);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_ffi_bitmap_add() -> anyhow::Result<()> {
        unsafe {
            let raw_vec = vec![0u8; 8];
            let ptr = raw_vec.as_ptr();
            let mut ffi_bm = FFIBitmap::new(ptr, 0, 64)?;
            ffi_bm.add(true)?;
            ffi_bm.add(false)?;
            ffi_bm.add(true)?;
            assert_eq!(3, ffi_bm.len());
            assert_eq!(64, ffi_bm.cap());
            assert_eq!(true, ffi_bm.get(0)?);
            assert_eq!(false, ffi_bm.get(1)?);
            assert_eq!(true, ffi_bm.get(2)?);
            ffi_bm.clear();
            assert_eq!(0, ffi_bm.len());
            ffi_bm.set_len(3)?;
            assert_eq!(3, ffi_bm.len());
            ffi_bm.set_len(64)?;
            assert!(ffi_bm.add(true).is_err());
            assert!(ffi_bm.set_len(65).is_err());
            Ok(())
        }
    }

    #[test]
    fn test_ffi_bitmap_range_iter() -> anyhow::Result<()> {
        use crate::bitmap::ReadBitmapExt;
        unsafe {
            let raw_vec = vec![1u8; 8];
            let ptr = raw_vec.as_ptr();
            let ffi_bm = FFIBitmap::new(ptr, 64, 64)?;
            let mut iter = ffi_bm.range_iter();
            assert_eq!((true, 1), iter.next().unwrap());
            assert_eq!((false, 7), iter.next().unwrap());
            Ok(())
        }
    }

    #[test]
    fn test_ffi_bitmap_extend() -> anyhow::Result<()> {
        use crate::bitmap::VecBitmap;
        unsafe {
            let raw_vec = vec![1u8; 8];
            let ptr = raw_vec.as_ptr();
            let mut ffi_bm = FFIBitmap::new(ptr, 0, 64)?;
            let vbm1 = VecBitmap::from(vec![true, false, true]);
            ffi_bm.extend(&vbm1)?;
            assert_eq!(vec![true, false, true], ffi_bm.bools().collect::<Vec<_>>());
            ffi_bm.extend_bools(vec![true, true, false])?;
            assert_eq!(
                vec![true, false, true, true, true, false],
                ffi_bm.bools().collect::<Vec<_>>()
            );
            ffi_bm.extend_const(true, 3)?;
            assert_eq!(
                vec![true, false, true, true, true, false, true, true, true],
                ffi_bm.bools().collect::<Vec<_>>()
            );
            ffi_bm.extend_range(&vbm1, 0..2)?;
            assert_eq!(
                vec![true, false, true, true, true, false, true, true, true, true, false],
                ffi_bm.bools().collect::<Vec<_>>()
            );
            ffi_bm.extend_const(false, 1)?;
            assert_eq!(
                vec![true, false, true, true, true, false, true, true, true, true, false, false],
                ffi_bm.bools().collect::<Vec<_>>()
            );

            assert!(ffi_bm.extend_range(&vbm1, 0..8).is_err());
            let bigbm = VecBitmap::from(vec![true; 64]);
            assert!(ffi_bm.extend_range(&bigbm, 0..64).is_err());
            assert!(ffi_bm.extend(&bigbm).is_err());
            assert!(ffi_bm.extend_const(true, 64).is_err());
            Ok(())
        }
    }
}
