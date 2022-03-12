mod vec;
mod view;

pub use vec::VecArray;
pub use view::ViewArray;

pub trait ArrayCast {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn cast_i32s(&self) -> &[i32];
}

pub trait ArrayBuild {
    /// Build i32 mutable slice for update, the length of returned
    /// slice is guaranteed to be equal to the given length.
    ///
    /// User can update values in returned slice and should not
    /// rely on its original contents.
    fn build_i32s(&mut self, len: usize) -> &mut [i32];
}
