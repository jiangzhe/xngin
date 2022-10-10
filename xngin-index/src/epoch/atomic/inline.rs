use std::marker::PhantomData;
use super::{Guard, Shared, Pointable, PointerOrInline, compose_inline_tag, decompose_inline_tag};
use std::fmt;

pub struct Inline<T> {
    pub(super) data: *mut (),
    _marker: PhantomData<T>,
}

impl<T: Pointable> crate::epoch::sealed::Sealed for Inline<T> {}
impl<T: Pointable> PointerOrInline<T> for Inline<T> {
    const MUST_BE_PTR: bool = false;

    #[inline]
    fn into_ptr(self) -> *mut () {
        self.data
    }

    #[inline]
    unsafe fn from_ptr(ptr: *mut ()) -> Self {
        Self {data: ptr, _marker: PhantomData}
    }
}

impl<T: Pointable> Inline<T> {
    pub fn new(data: usize) -> Self {
        debug_assert!(data.leading_zeros() >= T::ALIGN.trailing_zeros());
        Inline{
            data: compose_inline_tag::<T>((data << T::ALIGN.trailing_zeros()) as *mut (), 0) as *mut (),
            _marker: PhantomData,
        }
    }

    pub fn into_shared<'g>(self, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_ptr(self.into_ptr()) }
    }

    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_inline_tag::<T>(self.data);
        tag
    }

    pub fn with_tag(self, tag: usize) -> Self {
        let (ptr, _) = decompose_inline_tag::<T>(self.data);
        Inline{
            data: compose_inline_tag::<T>(ptr as *mut (), tag) as *mut (),
            _marker: PhantomData,
        } 
    }

    pub(crate) fn decompose_value(&self) -> (usize, usize) {
        let (ptr, tag) = decompose_inline_tag::<T>(self.data);
        (ptr >> T::ALIGN.trailing_zeros(), tag)
    }

    pub fn value(&self) -> usize {
        let (data, _) = self.decompose_value();
        data
    }
}

impl<T: Pointable> fmt::Debug for Inline<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (data, tag) = self.decompose_value();
        f.debug_struct("Inline")
            .field("data", &data)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: Pointable> Clone for Inline<T> {
    fn clone(&self) -> Self {
        Inline{data: self.data, _marker: PhantomData}
    }
}
