use crate::array::ArrayCast;
use crate::byte_repr::ByteRepr;

/// ViewArray represents a immutable view on an array without type info.
/// Use it separately is very unsafe.
/// Instead, bind it with exact data type.
/// The reason to define such a struct is to share with other data structures
/// within a continuous memory area.
/// For example, we can arrange multiple arrays together and construct a data
/// block to represent a "result set" or partial "result set".
/// The memory might be directly read from disk into a Vec<u8>, or just mmap()
/// to a raw pointer with specified length.
/// The FFIArray can directly point to some area and extract real data by
/// given a type, e.g. &\[i32\], &\[u64\], etc.
/// In this way, we achieve both dynamic types and high performance.
///
/// This type cannot implement Clone or Copy, as it will make dangling pointer
/// possible.
#[derive(Debug)]
pub struct ViewArray {
    ptr: *const (),
    len: usize,
    n_bytes: usize, // total bytes of underlying array
}

impl ViewArray {
    #[inline]
    pub fn raw(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.n_bytes) }
    }
}

impl ArrayCast for ViewArray {
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn cast<T: ByteRepr>(&self) -> &[T] {
        // High-level operations should ensure the pointer and length are valid.
        unsafe { std::slice::from_raw_parts(self.ptr as *const T, self.len()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_array_as_i32s() {
        let mut vec = vec![1, 2, 3];
        vec.shrink_to_fit();
        let len = vec.len();
        let leak = vec.leak();
        let arr = ViewArray {
            ptr: leak.as_ptr() as *const (),
            len,
            n_bytes: leak.len() * std::mem::size_of::<i32>(),
        };
        unsafe {
            let i32s = arr.cast::<i32>();
            assert_eq!(vec![1, 2, 3], i32s);
            // release leaked memory
            let _ = Vec::from_raw_parts(i32s.as_ptr() as *mut i32, i32s.len(), i32s.len());
        }
    }
}
