macro_rules! impl_sized_pointable {
    ($ty:ident < $($pty:tt),+ >) => {
        impl<$($pty),+> $crate::epoch::atomic::Pointable for $ty<$($pty),+> {
            const ALIGN: usize = std::mem::align_of::<Self>();
            type Init = Self;

            unsafe fn init(init: Self) -> *mut () {
                Box::into_raw(Box::new(init)).cast::<()>()
            }

            unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
                &*(ptr as *const Self)
            }

            unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
                &mut *ptr.cast::<Self>()
            }

            unsafe fn drop(ptr: *mut (), _: usize) {
                drop(Box::from_raw(ptr.cast::<Self>()));
            }
        }
    };
    ($ty:ty) => {
        impl $crate::epoch::atomic::Pointable for $ty {
            const ALIGN: usize = std::mem::align_of::<$ty>();
            type Init = Self;

            unsafe fn init(init: Self) -> *mut () {
                Box::into_raw(Box::new(init)).cast::<()>()
            }

            unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
                &*(ptr as *const Self)
            }

            unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
                &mut *ptr.cast::<Self>()
            }

            unsafe fn drop(ptr: *mut (), _: usize) {
                drop(Box::from_raw(ptr.cast::<Self>()));
            }
        }
    }
}
