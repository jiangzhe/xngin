use std::marker::PhantomData;

pub const COLD_BIT: u64 = 1u64 << 63;
pub const COLD_MASK: u64 = !COLD_BIT;
pub const COOL_BIT: u64 = 1u64 << 62;
pub const COOL_MASK: u64 = !COOL_BIT;
pub const HOT_MASK: u64 = !(3u64 << 62);

#[derive(Clone)]
pub struct SwizPtr<T> {
    val: u64,
    _marker: PhantomData<*mut BufferFrame<T>>,
}

impl<T> SwizPtr<T> {
    /// Create a new swizzled pointer with given raw pointer.
    #[inline]
    pub fn new_bf(ptr: *mut BufferFrame<T>) -> Self {
        SwizPtr {
            val: ptr as u64,
            _marker: PhantomData,
        }
    }

    /// Returns the in-memory pointer.
    #[inline]
    pub fn as_bf(&self) -> *mut BufferFrame<T> {
        self.val as *mut BufferFrame<T>
    }

    /// Returns the in-memory pointer.
    /// Usually used for data in cool stage.
    #[inline]
    pub fn as_bf_masked(&self) -> *mut BufferFrame<T> {
        (self.val & HOT_MASK) as *mut BufferFrame<T>
    }

    /// Returns the on-disk location identifier.
    #[inline]
    pub fn as_pid(&self) -> u64 {
        self.val & COLD_MASK
    }

    /// Returns whether the pointed data is hot(in memory).
    #[inline]
    pub fn is_hot(&self) -> bool {
        self.val & (COLD_BIT | COOL_BIT) == 0
    }

    /// Returns whether the pointed data is cold(on disk).
    #[inline]
    pub fn is_cold(&self) -> bool {
        self.val & COLD_BIT != 0
    }

    /// Returns whether the pointed data is cool.
    #[inline]
    pub fn is_cool(&self) -> bool {
        self.val & COOL_BIT != 0
    }

    /// Returns the raw value.
    #[inline]
    pub fn raw(&self) -> u64 {
        self.val
    }

    /// mark the pointer as cold.
    #[inline]
    pub fn mark_as_cold(&mut self, pid: u64) {
        self.val = pid | COLD_BIT;
    }

    /// mark the pointer from cool to hot.
    #[inline]
    pub fn warm(&mut self) {
        debug_assert!(self.is_cool());
        self.val = self.val & COOL_MASK;
    }
}

pub struct BufferFrame<T> {
    _marker: PhantomData<*mut T>,
}
