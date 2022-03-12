use crate::array::ArrayBuild;
use crate::array::ArrayCast;
use std::mem::size_of;

/// VecArray represents the owned version of an array of dynamic type.
#[derive(Default)]
pub struct VecArray {
    /// Vector to store arbitrary elements using binary format.
    /// The implicit conversion uses native endianness, which must be
    /// considered if data is to be persisted in external storage.
    inner: Vec<u8>,
    /// Original length of typed values.
    /// This is different from the byte length of inner vector.
    len: usize,
}

impl ArrayCast for VecArray {
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn cast_i32s(&self) -> &[i32] {
        assert!(self.len * size_of::<i32>() == self.inner.len());
        // # SAFETY
        //
        // Length is guaranteed to be valid as above assertion succeeds.
        unsafe { std::slice::from_raw_parts(self.inner.as_ptr() as *const i32, self.len()) }
    }
}

impl ArrayBuild for VecArray {
    #[inline]
    fn build_i32s(&mut self, len: usize) -> &mut [i32] {
        let len_u8 = len * size_of::<i32>();
        if self.inner.capacity() < len_u8 {
            self.inner.reserve_exact(len_u8 - self.len());
        }
        // # SAFETY
        //
        // length is ensured to be less than or equal to capacity
        unsafe {
            self.inner.set_len(len_u8);
            self.len = len;
            std::slice::from_raw_parts_mut(self.inner.as_mut_ptr() as *mut i32, len)
        }
    }
}

impl<T> From<Vec<T>> for VecArray {
    fn from(mut src: Vec<T>) -> Self {
        use std::mem::ManuallyDrop;
        // zero sized typed are not allowed
        assert!(size_of::<T>() > 1);
        let len = src.len();
        let len_u8 = len * size_of::<T>();
        let cap_u8 = src.capacity() * size_of::<T>();
        // Safety:
        //
        // Pointer, length and capacity are ensured to be valid to construct new byte vector.
        let inner = unsafe {
            let tgt = Vec::from_raw_parts(src.as_mut_ptr() as *mut u8, len_u8, cap_u8);
            let _ = ManuallyDrop::new(src);
            tgt
        };
        VecArray { inner, len }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_array_from_i32s() {
        let i32s = vec![1, 2, 3];
        let arr = VecArray::from(i32s);
        assert_eq!(3, arr.len());
        let mut bytes = vec![0; 12];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        assert_eq!(arr.inner, bytes);
    }

    #[test]
    fn test_vec_array_build_i32s() {
        let mut arr = VecArray::default();
        let i32s = arr.build_i32s(3);
        i32s.copy_from_slice(&[1, 2, 3]);
        let mut bytes = vec![0; 12];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        assert_eq!(arr.inner, bytes);
    }
}
