mod nonsized;
mod sized;
mod typed;

use crate::bitmap::ReadBitmap;
use crate::error::Result;
use std::ops::Range;
use xngin_datatype::DataType;

pub use self::typed::{TypedCol, VecF64Col, VecI64Col};

pub trait BaseCol {
    /// Returns how many values are stored in the column.
    fn len(&self) -> usize;

    /// Returns whether the column is empty.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether this column is type of fixed length.
    fn is_fixed_len(&self) -> bool;

    /// Returns whether this column is type of variable length.
    #[inline]
    fn is_var_len(&self) -> bool {
        !self.is_fixed_len()
    }

    /// Returns data type of this column.
    fn data_ty(&self) -> DataType;

    /// Returns whether the value at given position is valid.
    fn is_valid(&self, idx: usize) -> Result<bool>;

    /// Returns whether the value at given position is null.
    #[inline]
    fn is_null(&self, idx: usize) -> Result<bool> {
        self.is_valid(idx).map(|v| !v)
    }
}

pub trait ReadCol: BaseCol {
    type Val: ?Sized;

    /// Returns the value at given position.
    fn val(&self, idx: usize) -> Result<&Self::Val>;

    /// Optionally returns entire value slice, only support fixed-length data types.
    fn vals(&self) -> Option<&[Self::Val]>
    where
        Self::Val: Sized;

    /// Returns the valid map.
    fn vmap(&self) -> Option<&dyn ReadBitmap>;
}

pub trait WriteCol: ReadCol {
    /// Add a value to end of the column.
    fn add_val(&mut self, val: Self::Val) -> Result<()>;

    /// Add a value by ref to end of the column.
    fn add_ref_val(&mut self, val: &Self::Val) -> Result<()>;

    /// Add null to end of the column.
    fn add_null(&mut self) -> Result<()>;

    /// Returns the mutable reference of value at given position.
    fn val_mut(&mut self, idx: usize) -> Result<&mut Self::Val>;

    /// Set value at given position.
    fn set_val(&mut self, idx: usize, val: Self::Val) -> Result<()>;

    /// Set value by ref at given position.
    fn set_ref_val(&mut self, idx: usize, val: &Self::Val) -> Result<()>;

    /// Set null at given position.
    fn set_null(&mut self, idx: usize) -> Result<()>;

    /// Optionally returns the mutable value slice, only support fixed-length data types.
    fn vals_mut(&mut self) -> Option<&mut [Self::Val]>
    where
        Self::Val: Sized;
}

pub trait AppendCol: WriteCol {
    /// Extend nulls with given number.
    fn extend_null(&mut self, n: usize) -> Result<()>;

    /// Extend values with given number.
    fn extend_val(&mut self, val: &Self::Val, n: usize) -> Result<()>;

    /// Extend values from other column with entire range.
    fn extend_from_col(&mut self, other: &dyn ReadCol<Val = Self::Val>) -> Result<()>;

    /// Extend values from other column at given range.
    fn extend_from_col_range(
        &mut self,
        other: &dyn ReadCol<Val = Self::Val>,
        range: Range<usize>,
    ) -> Result<()>;

    /// Extend values from other column with given selective bitmap.
    fn extend_from_col_sel(
        &mut self,
        other: &dyn ReadCol<Val = Self::Val>,
        sel: &dyn ReadBitmap,
    ) -> Result<()>;
}
