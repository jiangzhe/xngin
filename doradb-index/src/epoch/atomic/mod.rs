mod inline;
mod owned;
mod shared;

use super::guard::Guard;
pub use inline::Inline;
pub use owned::Owned;
pub use shared::Shared;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

pub struct CompareExchangeError<'g, T: Pointable, P: PointerOrInline<T>> {
    pub current: Shared<'g, T>,
    pub new: P,
}

impl<T: Pointable, P: PointerOrInline<T> + fmt::Debug> fmt::Debug
    for CompareExchangeError<'_, T, P>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompareExchangeError")
            .field("current", &self.current)
            .field("new", &self.new)
            .finish()
    }
}

/// Types that are pointed to by a single word.
///
/// This trait differs from crossbeam-epoch, as it
/// only supports sized type.
///
/// If user wants to use dynamic sized type, he/she can
/// compose a sized struct with header and 0-length u8
/// array at end, embed the total length inside header
/// and handle alloc/dealloc accordingly.
/// In this way, user have more flexibilities to design
/// a compact header.
pub trait Pointable {
    const ALIGN: usize;
    type Init;

    unsafe fn init(init: Self::Init) -> *mut ();

    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self;

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self;

    unsafe fn drop(ptr: *mut (), tag: usize);
}

pub struct Atomic<T: ?Sized + Pointable> {
    data: AtomicPtr<()>,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T: Pointable + Send + Sync> Send for Atomic<T> {}
unsafe impl<T: Pointable + Send + Sync> Sync for Atomic<T> {}

impl<T> Atomic<T>
where
    T: Pointable<Init = T>,
{
    pub fn new(init: T) -> Atomic<T> {
        Self::from(Owned::new(init))
    }
}

impl<T: Pointable> Atomic<T> {
    fn from_ptr(data: *mut ()) -> Self {
        Self {
            data: AtomicPtr::new(data),
            _marker: PhantomData,
        }
    }

    pub const fn null() -> Atomic<T> {
        Self {
            data: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
        }
    }

    pub fn load<'g>(&self, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_ptr(self.data.load(ord)) }
    }

    pub fn store<P: PointerOrInline<T>>(&self, new: P, ord: Ordering) {
        self.data.store(new.into_ptr(), ord);
    }

    pub fn swap<'g, P: PointerOrInline<T>>(
        &self,
        new: P,
        ord: Ordering,
        _: &'g Guard,
    ) -> Shared<'g, T> {
        unsafe { Shared::from_ptr(self.data.swap(new.into_ptr(), ord)) }
    }

    pub fn fetch_or<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe {
            Shared::from_ptr(
                (*(&self.data as *const AtomicPtr<_> as *const AtomicUsize))
                    .fetch_or(val & low_bits::<T>(), ord) as *mut (),
            )
        }
    }

    pub fn compare_exchange<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: PointerOrInline<T>,
    {
        let new = new.into_ptr();
        self.data
            .compare_exchange(current.into_ptr(), new, success, failure)
            .map(|_| unsafe { Shared::from_ptr(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_ptr(current),
                    new: P::from_ptr(new),
                }
            })
    }

    pub fn compare_exchange_weak<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: PointerOrInline<T>,
    {
        let new = new.into_ptr();
        self.data
            .compare_exchange_weak(current.into_ptr(), new, success, failure)
            .map(|_| unsafe { Shared::from_ptr(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_ptr(current),
                    new: P::from_ptr(new),
                }
            })
    }
}

impl<T: Pointable> From<Owned<T>> for Atomic<T> {
    fn from(owned: Owned<T>) -> Self {
        let data = owned.data;
        mem::forget(owned);
        Self::from_ptr(data)
    }
}

impl<T: Pointable> From<Inline<T>> for Atomic<T> {
    fn from(inline: Inline<T>) -> Self {
        Self::from_ptr(inline.data)
    }
}

impl<T: Pointable> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (raw, tag) = decompose_tag::<T>(data);
        f.debug_struct("Atomic")
            .field("raw", &raw)
            .field("tag", &tag)
            .finish()
    }
}

pub trait PointerOrInline<T: Pointable>: super::sealed::Sealed {
    const MUST_BE_PTR: bool;

    fn into_ptr(self) -> *mut ();

    unsafe fn from_ptr(data: *mut ()) -> Self;
}

#[inline]
pub(self) fn ensure_aligned<T: ?Sized + Pointable>(raw: *mut ()) {
    assert_eq!(raw as usize & low_bits::<T>(), 0, "unaligned pointer");
}

#[inline]
pub fn low_bits<T: ?Sized + Pointable>() -> usize {
    (1 << T::ALIGN.trailing_zeros()) - 1
}

#[inline]
pub(self) fn compose_tag<T: ?Sized + Pointable>(ptr: *mut (), tag: usize) -> *mut () {
    int_to_ptr_with_provenance(
        (ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>()),
        ptr,
    )
}

#[inline]
pub(self) fn decompose_tag<T: ?Sized + Pointable>(ptr: *mut ()) -> (*mut (), usize) {
    (
        int_to_ptr_with_provenance(ptr as usize & !low_bits::<T>(), ptr),
        ptr as usize & low_bits::<T>(),
    )
}

#[inline]
pub(self) fn compose_inline_tag<T: ?Sized + Pointable>(ptr: *mut (), tag: usize) -> usize {
    (ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())
}

#[inline]
pub(self) fn decompose_inline_tag<T: ?Sized + Pointable>(ptr: *mut ()) -> (usize, usize) {
    (
        ptr as usize & !low_bits::<T>(),
        ptr as usize & low_bits::<T>(),
    )
}

#[inline]
fn int_to_ptr_with_provenance<T>(addr: usize, prov: *mut T) -> *mut T {
    let ptr = prov.cast::<u8>();
    ptr.wrapping_add(addr.wrapping_sub(ptr as usize)).cast()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_align() {
        #[repr(C, align(16))]
        struct A {
            data: u16,
        }

        let size = std::mem::size_of::<A>();
        let align = std::mem::align_of::<A>();
        println!("size={}, align={}", size, align);
        let a = A { data: 256 };
        let arr = unsafe { std::mem::transmute::<_, [u8; 16]>(a) };
        println!("arr={:?}", arr);
    }
}
