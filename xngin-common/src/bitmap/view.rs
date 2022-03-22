use crate::bitmap::ReadBitmap;
use crate::error::{Error, Result};

#[derive(Debug)]
pub struct ViewBitmap {
    ptr: *const u8,
    bits: usize,
    max_bits: usize,
}

impl ViewBitmap {
    /// Construct a ViewBitmap using raw pointer, with length and capacity.
    ///
    /// # Safety
    ///
    /// The caller must make sure the pointer is pointing to valid memory
    /// allocated by Rust allocator, and synchronize concurrent access.
    #[inline]
    pub unsafe fn new(ptr: *const u8, bits: usize, max_bits: usize) -> Result<Self> {
        if max_bits & 63 != 0 {
            return Err(Error::InvalidArgument(format!(
                "Capacity({}) of ViewBitmap is not multiply of 64",
                max_bits
            )));
        }
        if bits > max_bits {
            return Err(Error::InvalidArgument(format!(
                "Length of ViewBitmap({}) must be less than capacity({})",
                bits, max_bits
            )));
        }
        Ok(ViewBitmap {
            ptr,
            bits,
            max_bits,
        })
    }
}

impl ReadBitmap for ViewBitmap {
    #[inline]
    fn aligned_u64(&self) -> (&[u8], usize) {
        let aligned_len = ((self.bits + 63) >> 6) << 3;
        assert!(aligned_len * 8 <= self.max_bits);
        // # SAFETY
        // aligned length is guaranteed to be no more than max bits
        let bm = unsafe { std::slice::from_raw_parts(self.ptr, aligned_len) };
        (bm, self.bits)
    }

    #[inline]
    fn len(&self) -> usize {
        self.bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_bitmap_create_assertion() {
        unsafe {
            let raw_vec = vec![0u8; 8];
            let ptr = raw_vec.as_ptr();
            let res = ViewBitmap::new(ptr, 63, 63);
            assert!(res.is_err());
            let res = ViewBitmap::new(ptr, 128, 64);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_view_bitmap_range_iter() -> anyhow::Result<()> {
        unsafe {
            let raw_vec = vec![1u8; 8];
            let ptr = raw_vec.as_ptr();
            let ffi_bm = ViewBitmap::new(ptr, 64, 64)?;
            let mut iter = ffi_bm.range_iter();
            assert_eq!((true, 1), iter.next().unwrap());
            assert_eq!((false, 7), iter.next().unwrap());
            Ok(())
        }
    }
}
