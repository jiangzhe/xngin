use crate::alloc::{align_u128, RawArray};
use crate::byte_repr::ByteRepr;
use std::mem::size_of;
use std::slice::from_raw_parts;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Array {
    Owned {
        /// Aligned vector to store arbitrary elements using binary format.
        /// The implicit conversion uses native endianness, which must be
        /// considered if data is to be persisted in external storage.
        inner: RawArray,
        /// Original length of typed values.
        /// This is different from the byte length of inner vector.
        len: usize,
    },
    Borrowed {
        /// Shared pointer holding immutable byte array.
        ptr: Arc<[u8]>,
        /// Original length of typed values.
        len: usize,
        /// start offset from pointer
        start_bytes: usize,
        /// end offset from pointer
        end_bytes: usize,
    },
}

impl Array {
    /// create a new array with capacity of at least cap elment.
    #[inline]
    pub fn new_owned<T: ByteRepr>(cap: usize) -> Self {
        let cap_u8 = cap * size_of::<T>();
        let inner = RawArray::with_capacity(cap_u8);
        Array::Owned { inner, len: 0 }
    }

    /// Create a borrowed array.
    #[inline]
    pub fn new_borrowed<T: ByteRepr>(ptr: Arc<[u8]>, len: usize, start_bytes: usize) -> Self {
        debug_assert!((ptr.as_ptr() as usize + start_bytes) % std::mem::align_of::<T>() == 0); // check alignement
        let end_bytes = align_u128(start_bytes + std::mem::size_of::<T>() * len);
        debug_assert!(end_bytes <= ptr.len());
        Array::Borrowed {
            ptr,
            len,
            start_bytes,
            end_bytes,
        }
    }

    /// Returns count of original typed elements
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Array::Owned { len, .. } | Array::Borrowed { len, .. } => *len,
        }
    }

    /// Set length of this array.
    ///
    /// # SAFETY
    ///
    /// Caller must guarantee length is valid.
    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        match self {
            Array::Owned { len, .. } => *len = new_len,
            Array::Borrowed { .. } => panic!("Length of borrowed array is immutable"),
        }
    }

    /// Returns if the array is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn total_bytes(&self) -> usize {
        match self {
            Array::Owned { inner, .. } => inner.cap_u8(),
            Array::Borrowed {
                start_bytes,
                end_bytes,
                ..
            } => *end_bytes - *start_bytes,
        }
    }

    /// Returns raw byte slice.
    #[inline]
    pub fn raw(&self) -> &[u8] {
        match self {
            Array::Owned { inner, .. } => inner.as_slice(),
            Array::Borrowed {
                ptr,
                start_bytes,
                end_bytes,
                ..
            } => &ptr[*start_bytes..*end_bytes],
        }
    }

    /// Cast raw byte array to typed array.
    /// The given type `T` should be same as the type when constructing
    /// the array, or at least the type width should be identical.
    pub fn cast_slice<T: ByteRepr>(&self) -> &[T] {
        match self {
            Array::Owned { inner, len } => {
                debug_assert!(len * size_of::<T>() <= inner.cap_u8());
                // # SAFETY
                //
                // Length is guaranteed to be valid as above assertion succeeds.
                unsafe { inner.cast_slice::<T>(*len) }
            }
            Array::Borrowed {
                ptr,
                len,
                start_bytes,
                end_bytes,
            } => {
                debug_assert_eq!(*end_bytes, *start_bytes + len * size_of::<T>());
                // High-level operations should ensure the pointer and length are valid.
                unsafe { from_raw_parts(ptr.as_ptr().add(*start_bytes) as *const T, *len) }
            }
        }
    }

    /// Build mutable slice for update, the length of returned
    /// slice is guaranteed to be equal to the given length.
    ///
    /// User can update values in returned slice and should not
    /// rely on its original contents.
    pub fn cast_slice_mut<T: ByteRepr>(&mut self, len: usize) -> Option<&mut [T]> {
        match self {
            Array::Owned {
                inner,
                len: inner_len,
            } => {
                let len_u8 = len * size_of::<T>();
                if len_u8 > inner.cap_u8() {
                    // reallocate a new aligned vec
                    let mut new_inner = RawArray::with_capacity(len_u8);
                    let raw = inner.as_slice();
                    new_inner.as_slice_mut()[..raw.len()].copy_from_slice(raw);
                    *inner = new_inner;
                }
                *inner_len = len;
                // # SAFETY
                //
                // length is ensured to be less than or equal to capacity.
                Some(unsafe { inner.cast_slice_mut(len) })
            }
            Array::Borrowed { .. } => None,
        }
    }

    /// Convert the array to owned.
    /// If it's already owned, this call is no-op.
    #[inline]
    pub fn to_mut(&mut self) -> &mut Self {
        match self {
            Array::Owned { .. } => self,
            Array::Borrowed {
                ptr,
                len,
                start_bytes,
                end_bytes,
            } => {
                let len = *len;
                let raw = &ptr[*start_bytes..*end_bytes];
                let mut inner = RawArray::with_capacity(raw.len());
                inner.as_slice_mut()[..raw.len()].copy_from_slice(raw);
                *self = Array::Owned { inner, len };
                self
            }
        }
    }

    /// Clone self to owned with atomic reference.
    #[inline]
    pub fn clone_to_owned(this: &Arc<Self>) -> Arc<Self> {
        match this.as_ref() {
            Array::Owned { .. } => Arc::clone(this),
            Array::Borrowed {
                ptr,
                len,
                start_bytes,
                end_bytes,
            } => {
                let len = *len;
                let raw = &ptr[*start_bytes..*end_bytes];
                let mut inner = RawArray::with_capacity(raw.len());
                inner.as_slice_mut()[..raw.len()].copy_from_slice(raw);
                Arc::new(Array::Owned { inner, len })
            }
        }
    }
}

// This method is convenient for test, and may be not very performant
impl<T, I> From<I> for Array
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
        let mut inner = RawArray::with_capacity(cap_u8);
        let chunks = inner.as_slice_mut().chunks_exact_mut(size_of::<T>());
        for (chk, v) in chunks.zip(src) {
            v.write_bytes(chk);
        }
        Array::Owned { inner, len }
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
    fn test_borrowed_array() {
        let mut bytes = vec![0; 16];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        bytes[12..16].copy_from_slice(&4i32.to_ne_bytes());
        let input = bytes.clone();
        let raw: Arc<[u8]> = Arc::from(input.into_boxed_slice());
        let array = Array::new_borrowed::<i32>(raw, 4, 0);
        assert_eq!(16, array.total_bytes());
        assert!(!array.is_empty());
        assert_eq!(&bytes[..], array.raw());
        let mut a2 = array.clone();
        let a2 = a2.to_mut();
        assert_eq!(array.raw()[..16], a2.raw()[..16]);
        let a3 = Arc::new(array.clone());
        let a4 = Array::clone_to_owned(&a3);
        assert_eq!(a3.cast_slice::<i32>(), a4.cast_slice::<i32>());
    }

    #[test]
    fn test_owned_array_from_i32s() {
        let i32s: Vec<i32> = vec![1, 2, 3];
        let arr = Array::from(i32s.into_iter());
        assert_eq!(3, arr.len());
        let mut bytes = vec![0; 12];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        assert_eq!(
            bytemuck::cast_slice::<_, u8>(arr.cast_slice::<i32>()),
            &bytes[..]
        );
    }

    #[test]
    fn test_owned_array_build_i32s() {
        let mut arr = Array::new_owned::<i32>(3);
        let i32s = arr.cast_slice_mut::<i32>(3).unwrap();
        i32s.copy_from_slice(&[1, 2, 3]);
        let mut bytes = vec![0; 12];
        bytes[0..4].copy_from_slice(&1i32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&3i32.to_ne_bytes());
        assert_eq!(
            bytemuck::cast_slice::<_, u8>(arr.cast_slice::<i32>()),
            &bytes[..]
        );
    }

    #[test]
    fn test_owned_array_cast_more_cap() {
        let mut arr = Array::new_owned::<i32>(64);
        assert!(arr.cast_slice_mut::<i32>(128).is_some());
        assert_eq!(128, arr.len());
    }

    #[test]
    fn test_borrowed_array_i32s() {
        let mut vec: Vec<u8> = Vec::with_capacity(12);
        vec.extend(1i32.to_ne_bytes());
        vec.extend(2i32.to_ne_bytes());
        vec.extend(3i32.to_ne_bytes());
        vec.extend(4i32.to_ne_bytes());
        let ptr: Arc<[u8]> = Arc::from(vec);
        let arr = Array::new_borrowed::<i32>(ptr, 4, 0);
        let i32s = arr.cast_slice::<i32>();
        assert_eq!(vec![1, 2, 3, 4], i32s);
    }
}
