use crate::align::AlignedVec;
use crate::array::view::ViewArray;
use crate::array::ArrayBuild;
use crate::array::ArrayCast;
use crate::byte_repr::ByteRepr;
use std::mem::size_of;

/// VecArray represents the owned version of an array of dynamic type.
#[derive(Debug, Clone, Default)]
pub struct VecArray {
    /// Aligned vector to store arbitrary elements using binary format.
    /// The implicit conversion uses native endianness, which must be
    /// considered if data is to be persisted in external storage.
    inner: AlignedVec,
    /// Original length of typed values.
    /// This is different from the byte length of inner vector.
    len: usize,
}

impl VecArray {
    #[inline]
    pub fn from_view(view: &ViewArray) -> Self {
        let len = view.len();
        let raw = view.raw();
        let mut inner = AlignedVec::with_capacity(raw.len());
        inner.as_slice_mut().copy_from_slice(raw);
        VecArray { inner, len }
    }
}

impl ArrayCast for VecArray {
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn cast<T: ByteRepr>(&self) -> &[T] {
        debug_assert!(self.len * size_of::<T>() <= self.inner.cap_u8());
        // # SAFETY
        //
        // Length is guaranteed to be valid as above assertion succeeds.
        unsafe { std::slice::from_raw_parts(self.inner.as_ptr() as *const T, self.len()) }
    }
}

impl ArrayBuild for VecArray {
    #[inline]
    fn new<T: ByteRepr>(cap: usize) -> Self {
        let cap_u8 = cap * size_of::<T>();
        let inner = AlignedVec::with_capacity(cap_u8);
        VecArray { inner, len: 0 }
    }

    #[inline]
    fn cast_mut<T: ByteRepr>(&mut self, len: usize) -> &mut [T] {
        let len_u8 = len * size_of::<T>();
        if len_u8 > self.inner.cap_u8() {
            // reallocate a new aligned vec
            let mut new_inner = AlignedVec::with_capacity(len_u8);
            let raw = self.inner.as_slice();
            new_inner.as_slice_mut()[..raw.len()].copy_from_slice(raw);
            self.inner = new_inner;
        }
        // # SAFETY
        //
        // length is ensured to be less than or equal to capacity
        unsafe {
            self.len = len;
            std::slice::from_raw_parts_mut(self.inner.as_mut_ptr() as *mut T, len)
        }
    }
}

// This method is convenient for test, and may be not very performant
impl<T, I> From<I> for VecArray
where
    T: ByteRepr,
    I: ExactSizeIterator<Item = T>,
{
    #[inline]
    fn from(src: I) -> Self {
        // zero sized typed are not allowed
        assert!(size_of::<T>() > 0);
        let len = src.len();
        let cap_u8 = len * size_of::<T>();
        // let mut arr = VecArray::new::<T>(cap_u8);
        let mut inner = AlignedVec::with_capacity(cap_u8);
        let chunks = inner.as_slice_mut().chunks_exact_mut(size_of::<T>());
        for (chk, v) in chunks.zip(src) {
            v.write_bytes(chk);
        }
        VecArray { inner, len }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_align() {
        use std::mem::{align_of, size_of};
        #[repr(C, align(64))]
        struct OneByteAlign64;
        println!(
            "align_of::<OneByteAlign64>() = {}",
            align_of::<OneByteAlign64>()
        );
        println!(
            "size_of::<OneByteAlign64>() = {}",
            size_of::<OneByteAlign64>()
        );
    }

    #[test]
    fn test_vec_array_from_i32s() {
        let i32s: Vec<i32> = vec![1, 2, 3];
        let arr = VecArray::from(i32s.into_iter());
        assert_eq!(3, arr.len());
        let mut bytes = vec![0; 12];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        assert_eq!(bytemuck::cast_slice::<_, u8>(arr.cast::<i32>()), &bytes[..]);
    }

    #[test]
    fn test_vec_array_build_i32s() {
        let mut arr = VecArray::new::<i32>(3);
        let i32s = arr.cast_mut::<i32>(3);
        i32s.copy_from_slice(&[1, 2, 3]);
        let mut bytes = vec![0; 12];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        assert_eq!(bytemuck::cast_slice::<_, u8>(arr.cast::<i32>()), &bytes[..]);
    }
}
