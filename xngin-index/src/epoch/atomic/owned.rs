use super::{
    compose_tag, decompose_tag, ensure_aligned, Guard, Pointable, PointerOrInline, Shared,
};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};

pub struct Owned<T: Pointable> {
    pub(super) data: *mut (),
    _marker: PhantomData<Box<T>>,
}

impl<T: Pointable> crate::epoch::sealed::Sealed for Owned<T> {}
impl<T: Pointable> PointerOrInline<T> for Owned<T> {
    const MUST_BE_PTR: bool = true;

    #[inline]
    fn into_ptr(self) -> *mut () {
        let data = self.data;
        mem::forget(self);
        data
    }

    #[inline]
    unsafe fn from_ptr(data: *mut ()) -> Self {
        debug_assert!(!data.is_null(), "converting null into `Owned`");
        Self {
            data,
            _marker: PhantomData,
        }
    }
}

impl<T: Pointable<Init = T>> Owned<T> {
    pub fn new(init: T) -> Owned<T> {
        unsafe { Self::from_ptr(T::init(init)) }
    }
}

impl<T: Pointable> Owned<T> {
    pub fn new_dyn(init: T::Init) -> Owned<T> {
        unsafe { Self::from_ptr(T::init(init)) }
    }
}

impl<T: Pointable> Owned<T> {
    pub fn into_box(self) -> Box<T> {
        let (raw, _) = decompose_tag::<T>(self.data);
        mem::forget(self);
        unsafe { Box::from_raw(raw.cast::<T>()) }
    }

    pub fn into_shared(self, _: &Guard) -> Shared<'_, T> {
        unsafe { Shared::from_ptr(self.into_ptr()) }
    }

    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_tag::<T>(self.data);
        tag
    }

    pub fn with_tag(self, tag: usize) -> Owned<T> {
        let data = self.into_ptr();
        unsafe { Self::from_ptr(compose_tag::<T>(data, tag)) }
    }

    pub(crate) fn decompose(&self) -> (*mut (), usize) {
        decompose_tag::<T>(self.data)
    }
}

impl<T: Pointable> Drop for Owned<T> {
    fn drop(&mut self) {
        let (raw, tag) = decompose_tag::<T>(self.data);
        unsafe { T::drop(raw, tag) }
    }
}

impl<T: Pointable> fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (raw, tag) = decompose_tag::<T>(self.data);
        f.debug_struct("Owned")
            .field("raw", &raw)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: Pointable<Init = T> + Clone> Clone for Owned<T> {
    fn clone(&self) -> Self {
        let owned: Owned<T> = Owned::new((**self).clone());
        owned.with_tag(self.tag())
    }
}

impl<T: Pointable> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        let (raw, _) = decompose_tag::<T>(self.data);
        unsafe { T::deref(raw) }
    }
}

impl<T: Pointable> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        let (raw, _) = decompose_tag::<T>(self.data);
        unsafe { T::deref_mut(raw) }
    }
}
