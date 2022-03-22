use std::mem::{size_of, ManuallyDrop};
use std::ptr::NonNull;

#[repr(C, align(64))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Alignment([u64; 8]);

pub const ALIGN64B: usize = size_of::<Alignment>();

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlignedVec {
    ptr: NonNull<Alignment>,
    cap_u8: usize,
}

impl Drop for AlignedVec {
    #[inline]
    fn drop(&mut self) {
        // # SAFETY
        //
        // The pointer and capacity are guaranteed to be always valid.
        unsafe {
            let cap_u512 = self.cap_u8 / ALIGN64B;
            let _ = Vec::from_raw_parts(self.ptr.as_mut(), cap_u512, cap_u512);
        }
    }
}

impl AlignedVec {
    #[inline]
    pub fn with_capacity(cap_u8: usize) -> Self {
        // we don't allow empty capacity.
        let cap_u8 = if cap_u8 == 0 { 1 } else { cap_u8 };
        let cap_u512 = (cap_u8 + ALIGN64B - 1) / ALIGN64B;
        let mut inner = Vec::with_capacity(cap_u512);
        // # SAFETY
        //
        // Vec is not allowed to be empty.
        // Once the vec is initialized, it's not allowed to reallocate.
        // so the pointer and capacity are guaranteed to be valid.
        unsafe {
            let cap_u8 = inner.capacity() * ALIGN64B;
            let ptr = NonNull::new_unchecked(inner.as_mut_ptr());
            let _ = ManuallyDrop::new(inner);
            AlignedVec { ptr, cap_u8 }
        }
    }

    /// # Safety
    ///
    /// Caller must uphold the invariant that the pointer is valid, correctly aligned,
    /// capacity is valid, ownership is correctly passed.
    #[inline]
    pub unsafe fn from_raw_parts(ptr: *mut Alignment, cap_u8: usize) -> Self {
        AlignedVec {
            ptr: NonNull::new_unchecked(ptr),
            cap_u8,
        }
    }

    #[inline]
    pub fn cap_u8(&self) -> usize {
        self.cap_u8
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // # SAFETY
        //
        // AlignedVec is not allowed to realloc, so capacity is always valid.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const u8, self.cap_u8) }
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        // # SAFETY
        //
        // AlignedVec is not allowed to realloc, so capacity is always valid.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr() as *mut u8, self.cap_u8) }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const Alignment {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut Alignment {
        self.ptr.as_ptr()
    }
}

impl Default for AlignedVec {
    #[inline]
    fn default() -> Self {
        AlignedVec::with_capacity(64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment() {
        use std::mem::{align_of, size_of};
        println!("align_of::<Alignment>() = {}", align_of::<Alignment>());
        println!("size_of::<Alignment>() = {}", size_of::<Alignment>());
        let vec = AlignedVec::with_capacity(1);
        println!(
            "ptr of AlignedVec aligned to 64B? {}",
            vec.as_slice().as_ptr() as usize & 63 == 0
        );
    }
}
