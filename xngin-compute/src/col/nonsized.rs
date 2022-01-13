use crate::bitmap::VecBitmap;
use crate::col::BaseCol;
use crate::types::{DataType, Typed};
use std::marker::PhantomData;

#[allow(dead_code)]
pub struct VecNonsizedCol<T: ?Sized> {
    vmap: Option<VecBitmap>,
    data: Vec<u8>,
    offsets: Vec<usize>,
    _marker: PhantomData<T>,
}

impl<T: Typed + ?Sized + 'static> BaseCol for VecNonsizedCol<T> {
    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn is_fixed_len(&self) -> bool {
        false
    }

    #[inline]
    fn data_ty(&self) -> DataType {
        T::ty()
    }

    fn is_valid(&self, _idx: usize) -> crate::error::Result<bool> {
        todo!()
    }
}
