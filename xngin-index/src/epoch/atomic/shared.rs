use std::marker::PhantomData;
use super::{Owned, Pointable, Inline, PointerOrInline, compose_tag, decompose_tag, ensure_aligned};
use std::ptr;
use std::fmt;

pub struct Shared<'g, T: 'g + ?Sized + Pointable> {
    data: *mut (),
    _marker: PhantomData<(&'g (), *const T)>,
}

impl<T: ?Sized + Pointable> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<T: Pointable> Copy for Shared<'_, T> {}

impl<T: Pointable> crate::epoch::sealed::Sealed for Shared<'_, T> {}

impl<T: Pointable> PointerOrInline<T> for Shared<'_, T> {
    const MUST_BE_PTR: bool = false;

    #[inline]
    fn into_ptr(self) -> *mut () {
        self.data
    }

    #[inline]
    unsafe fn from_ptr(data: *mut ()) -> Self {
        Shared {
            data,
            _marker: PhantomData,
        }
    }
}

impl<'g, T: Pointable> Shared<'g, T> {
    pub fn as_raw(&self) -> *const T {
        let (raw, _) = decompose_tag::<T>(self.data);
        raw as *const _
    }
}

impl<'g, T: 'g + Pointable> Shared<'g, T> {
    pub fn null() -> Self {
        Shared {
            data: ptr::null_mut(),
            _marker: PhantomData,
        }
    }

    pub fn is_null(&self) -> bool {
        let (raw, _) = decompose_tag::<T>(self.data);
        raw.is_null()
    }

    pub unsafe fn deref(&self) -> &'g T {
        let (raw, _) = decompose_tag::<T>(self.data);
        T::deref(raw)
    }

    pub unsafe fn deref_mut(&mut self) -> &'g T {
        let (raw, _) = decompose_tag::<T>(self.data);
        T::deref_mut(raw)
    }

    pub unsafe fn as_ref(&self) -> Option<&'g T> {
        let (raw, _) = decompose_tag::<T>(self.data);
        if raw.is_null() {
            None
        } else {
            Some(T::deref(raw))
        }
    }

    pub unsafe fn try_into_owned(self) -> Option<Owned<T>> {
        if self.is_null() {
            None
        } else {
            Some(Owned::from_ptr(self.data))
        }
    }

    pub unsafe fn into_inline(self) -> Inline<T> {
        Inline::from_ptr(self.data)
    }

    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_tag::<T>(self.data);
        tag
    }

    pub fn with_tag(&self, tag: usize) -> Shared<'g, T> {
        unsafe { Self::from_ptr(compose_tag::<T>(self.data, tag)) }
    }

    pub(crate) fn decompose(&self) -> (*mut (), usize) {
        decompose_tag::<T>(self.data)
    }
}

impl<T: Pointable> Default for Shared<'_, T> {
    fn default() -> Self {
        Shared::null()
    }
}

impl<T: Pointable> fmt::Debug for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (raw, tag) = decompose_tag::<T>(self.data);
        f.debug_struct("Shared")
            .field("raw", &raw)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: Pointable> fmt::Pointer for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Pointer::fmt(&(unsafe {self.deref() as *const _}), f)
    }
}

impl<T: Pointable> From<*const T> for Shared<'_, T> {
    fn from(raw: *const T) -> Self {
        let raw = raw as *mut ();
        ensure_aligned::<T>(raw);
        unsafe { Self::from_ptr(raw) }
    }
}

impl<'g, T: ?Sized + Pointable> PartialEq<Shared<'g, T>> for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T: ?Sized + Pointable> Eq for Shared<'_, T> {}
