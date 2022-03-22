mod vec;
mod view;

use crate::byte_repr::ByteRepr;
pub use vec::VecArray;
pub use view::ViewArray;

pub trait ArrayCast {
    /// Returns count of original typed elements
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cast raw byte array to typed array.
    /// The given type `T` should be same as the type when constructing
    /// the array, or at least the type width should be identical.
    fn cast<T: ByteRepr>(&self) -> &[T];
}

pub trait ArrayBuild {
    /// create a new array with capacity of at least cap elment.
    fn new<T: ByteRepr>(cap: usize) -> Self;

    /// Build mutable slice for update, the length of returned
    /// slice is guaranteed to be equal to the given length.
    ///
    /// User can update values in returned slice and should not
    /// rely on its original contents.
    fn cast_mut<T: ByteRepr>(&mut self, len: usize) -> &mut [T];
}
