use crate::bitmap::{AppendBitmap, ReadBitmap, ReadBitmapExt, VecBitmap, WriteBitmap};
use crate::col::{AppendCol, BaseCol, ReadCol, WriteCol};
use crate::error::{Error, Result};
use crate::types::{DataType, Typed};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct VecSizedCol<T> {
    vmap: Option<VecBitmap>,
    data: Vec<T>,
}

impl<T: Typed> Default for VecSizedCol<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Typed> VecSizedCol<T> {
    #[inline]
    pub fn new() -> Self {
        VecSizedCol {
            vmap: None,
            data: Vec::new(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        VecSizedCol {
            vmap: None,
            data: Vec::with_capacity(capacity),
        }
    }

    /// Consumes the column, and returns the data array.
    /// NOTE: Valid map is ignored.
    #[inline]
    pub fn into_data(self) -> Vec<T> {
        self.data
    }

    #[inline]
    fn extend_vmap(&mut self, vmap: Option<&dyn ReadBitmap>, size: usize) -> Result<()> {
        match (self.vmap.as_mut(), vmap) {
            (None, None) => Ok(()),
            (None, Some(ovm)) => {
                let mut vm = VecBitmap::with_capacity(self.len() + size);
                vm.extend_const(true, self.len())?;
                vm.extend(ovm)?;
                self.vmap = Some(vm);
                Ok(())
            }
            (Some(svm), None) => svm.extend_const(true, size),
            (Some(svm), Some(ovm)) => svm.extend(ovm),
        }
    }

    #[inline]
    fn extend_vmap_range(
        &mut self,
        vmap: Option<&dyn ReadBitmap>,
        range: Range<usize>,
    ) -> Result<()> {
        match (self.vmap.as_mut(), vmap) {
            (None, None) => Ok(()),
            (None, Some(ovm)) => {
                let mut vm = VecBitmap::with_capacity(self.len() + range.end - range.start);
                vm.extend_const(true, self.len())?;
                vm.extend_range(ovm, range)?;
                self.vmap = Some(vm);
                Ok(())
            }
            (Some(svm), None) => svm.extend_const(true, range.end - range.start),
            (Some(svm), Some(ovm)) => svm.extend_range(ovm, range),
        }
    }
}

impl<T: Typed> From<Vec<T>> for VecSizedCol<T> {
    fn from(data: Vec<T>) -> Self {
        VecSizedCol { vmap: None, data }
    }
}

impl<T: Typed> BaseCol for VecSizedCol<T> {
    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn is_fixed_len(&self) -> bool {
        true
    }

    #[inline]
    fn data_ty(&self) -> DataType {
        T::ty()
    }

    #[inline]
    fn is_valid(&self, idx: usize) -> Result<bool> {
        if idx >= self.data.len() {
            return Err(Error::IndexOutOfBound(format!(
                "{} no less than column length",
                idx
            )));
        }
        self.vmap.as_ref().map(|vm| vm.get(idx)).unwrap_or(Ok(true))
    }
}

impl<T: Typed> ReadCol for VecSizedCol<T> {
    type Val = T;

    #[inline]
    fn val(&self, idx: usize) -> Result<&Self::Val> {
        self.data.get(idx).ok_or_else(|| {
            Error::IndexOutOfBound(format!(
                "column index {} greater than length {}",
                idx,
                self.len()
            ))
        })
    }

    #[inline]
    fn vmap(&self) -> Option<&dyn ReadBitmap> {
        self.vmap.as_ref().map(|vm| vm as &dyn ReadBitmap)
    }

    #[inline]
    fn vals(&self) -> Option<&[Self::Val]> {
        Some(&self.data)
    }
}

impl<T: Typed + Copy + Default> WriteCol for VecSizedCol<T> {
    #[inline]
    fn add_val(&mut self, val: Self::Val) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.add(true)?;
        }
        self.data.push(val);
        Ok(())
    }

    #[inline]
    fn add_ref_val(&mut self, val: &Self::Val) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.add(true)?;
        }
        self.data.push(*val);
        Ok(())
    }

    #[inline]
    fn add_null(&mut self) -> Result<()> {
        // self.vmap.add_valid(false)?;
        if let Some(vm) = self.vmap.as_mut() {
            vm.add(false)?;
        } else {
            let mut vm = VecBitmap::with_capacity(self.len() + 1);
            vm.extend_const(true, self.len())?;
            vm.add(false)?;
            self.vmap = Some(vm);
        }
        self.data.push(Default::default()); // align data slice.
        Ok(())
    }

    #[inline]
    fn val_mut(&mut self, idx: usize) -> Result<&mut Self::Val> {
        self.data.get_mut(idx).ok_or_else(|| {
            Error::IndexOutOfBound(format!("index {} greater than column length", idx))
        })
    }

    #[inline]
    fn set_val(&mut self, idx: usize, val: Self::Val) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.set(idx, true)?;
        }
        self.data.get_mut(idx).map(|v| *v = val).ok_or_else(|| {
            Error::IndexOutOfBound(format!("index {} greater than column length", idx))
        })
    }

    #[inline]
    fn set_ref_val(&mut self, idx: usize, val: &Self::Val) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.set(idx, true)?;
        }
        self.data.get_mut(idx).map(|v| *v = *val).ok_or_else(|| {
            Error::IndexOutOfBound(format!("index {} greater than column length", idx))
        })
    }

    #[inline]
    fn set_null(&mut self, idx: usize) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.set(idx, false)?;
        } else {
            let mut vm = VecBitmap::with_capacity(self.len());
            vm.extend_const(true, self.len())?;
            vm.set(idx, false)?;
            self.vmap = Some(vm);
        }
        Ok(())
    }

    #[inline]
    fn vals_mut(&mut self) -> Option<&mut [Self::Val]> {
        Some(&mut self.data)
    }
}

impl<T: Typed + Copy + Default> AppendCol for VecSizedCol<T> {
    #[inline]
    fn extend_null(&mut self, n: usize) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.extend_const(false, n)?;
        } else {
            let mut vm = VecBitmap::with_capacity(self.len() + n);
            vm.extend_const(true, self.len())?;
            vm.extend_const(false, n)?;
            self.vmap = Some(vm);
        }
        let new_len = self.data.len() + n;
        self.data.resize(new_len, Default::default());
        Ok(())
    }

    #[inline]
    fn extend_val(&mut self, val: &Self::Val, n: usize) -> Result<()> {
        if let Some(vm) = self.vmap.as_mut() {
            vm.extend_const(true, n)?;
        }
        let new_len = self.data.len() + n;
        self.data.resize(new_len, *val);
        Ok(())
    }

    #[inline]
    fn extend_from_col(&mut self, other: &dyn ReadCol<Val = Self::Val>) -> Result<()> {
        self.extend_vmap(other.vmap(), other.len())?;
        if let Some(slice) = other.vals() {
            self.data.extend_from_slice(slice);
        } else {
            for i in 0..other.len() {
                self.data.push(*other.val(i)?);
            }
        }
        Ok(())
    }

    #[inline]
    fn extend_from_col_range(
        &mut self,
        other: &dyn ReadCol<Val = Self::Val>,
        range: Range<usize>,
    ) -> Result<()> {
        self.extend_vmap_range(other.vmap(), range.clone())?;
        if let Some(slice) = other.vals() {
            self.data.extend_from_slice(&slice[range]);
        } else {
            for i in range {
                self.data.push(*other.val(i)?);
            }
        }
        Ok(())
    }

    #[inline]
    fn extend_from_col_sel(
        &mut self,
        other: &dyn ReadCol<Val = Self::Val>,
        sel: &dyn ReadBitmap,
    ) -> Result<()> {
        let other_vmap = other.vmap();
        let iter = sel.range_iter();
        let mut prev = 0usize;
        if let Some(slice) = other.vals() {
            for (flag, n) in iter {
                let end = prev + n;
                if flag {
                    // only append values which are valid(not null)
                    self.extend_vmap_range(other_vmap, prev..end)?;
                    self.data.extend_from_slice(&slice[prev..end]);
                }
                prev = end;
            }
        } else {
            for (flag, n) in iter {
                let end = prev + n;
                if flag {
                    self.extend_vmap_range(other_vmap, prev..end)?;
                    for i in prev..end {
                        self.data.push(*other.val(i)?);
                    }
                }
                prev = end;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::col::{BaseCol, ReadCol, WriteCol};
    use anyhow::Result;

    #[test]
    fn test_sized_col_ops1() -> Result<()> {
        let mut i64s = VecSizedCol::<i64>::new();
        assert!(i64s.is_fixed_len());
        i64s.add_val(1)?;
        assert_eq!(1, i64s.len());
        assert_eq!(1, *i64s.val(0)?);
        assert_eq!(&[1], i64s.vals().unwrap());
        i64s.add_val(2)?;
        i64s.add_val(3)?;
        let slice = i64s.vals_mut().unwrap();
        for v in slice {
            *v = 0;
        }
        assert_eq!(&[0, 0, 0], i64s.vals().unwrap());
        i64s.add_null()?;
        assert!(i64s.is_null(3)?);
        assert_eq!(
            vec![true, true, true, false],
            i64s.vmap().unwrap().bools().collect::<Vec<_>>()
        );
        i64s.set_null(1)?;
        assert!(i64s.is_null(1)?);
        i64s.set_val(1, 100)?;
        assert_eq!(100, *i64s.val(1)?);
        i64s.set_ref_val(1, &20)?;
        assert_eq!(20, *i64s.val(1)?);
        *i64s.val_mut(2)? = 10;
        assert_eq!(10, *i64s.val(2)?);
        {
            let vals = i64s.vals_mut().unwrap();
            vals[0] = 1;
            vals[1] = 2;
            vals[2] = 3;
            assert_eq!(
                vec![1, 2, 3],
                i64s.vals()
                    .unwrap()
                    .iter()
                    .cloned()
                    .take(3)
                    .collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    #[test]
    fn test_sized_col_ops2() -> Result<()> {
        let mut i64s = VecSizedCol::<i64>::new();
        i64s.add_null()?;
        i64s.add_null()?;
        assert_eq!(2, i64s.len());
        i64s = VecSizedCol::<i64>::new();
        i64s.extend_val(&1, 3)?;
        i64s.set_null(1)?;
        assert!(i64s.is_null(1)?);
        i64s.extend_null(3)?;
        assert_eq!(6, i64s.len());
        Ok(())
    }

    #[test]
    fn test_sized_col_ops3() -> Result<()> {
        let mut i64s = VecSizedCol::<i64>::new();
        i64s.add_ref_val(&1)?;
        i64s.add_ref_val(&2)?;
        i64s.add_val(3)?;
        assert_eq!(3, i64s.len());
        Ok(())
    }

    #[test]
    fn test_sized_col_extend1() -> Result<()> {
        let mut i64s = VecSizedCol::<i64>::new();
        i64s.extend_null(3)?;
        assert!(i64s.is_null(0)?);
        assert!(i64s.is_null(1)?);
        assert!(i64s.is_null(2)?);
        i64s.extend_val(&1, 2)?;
        assert_eq!(1, *i64s.val(3)?);
        assert_eq!(1, *i64s.val(4)?);
        let tmp = VecSizedCol::from(vec![6, 7, 8, 9, 10]);
        i64s.extend_from_col(&tmp)?;
        assert_eq!(10, i64s.len());
        i64s.extend_from_col_range(&tmp, 0..2)?;
        assert_eq!(12, i64s.len());
        let sel = VecBitmap::from(vec![true, false, true, false, false]);
        i64s.extend_from_col_sel(&tmp, &sel)?;
        assert_eq!(14, i64s.len());
        assert_eq!(
            vec![1, 1, 6, 7, 8, 9, 10, 6, 7, 6, 8],
            i64s.vals()
                .unwrap()
                .iter()
                .cloned()
                .skip(3)
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_sized_col_extend2() -> Result<()> {
        let mut i64s = VecSizedCol::<i64>::new();
        i64s.extend_val(&1, 3)?;
        let mut tmp = VecSizedCol::<i64>::from(vec![4, 5, 6]);
        i64s.extend_from_col(&tmp)?;
        tmp.add_null()?;
        i64s.extend_from_col(&tmp)?;
        assert_eq!(10, i64s.len());
        i64s.extend_from_col(&tmp)?;
        assert_eq!(14, i64s.len());
        Ok(())
    }

    #[test]
    fn test_sized_col_extend3() -> Result<()> {
        let mut i64s = VecSizedCol::<i64>::new();
        i64s.extend_val(&1, 3)?;
        let mut tmp = VecSizedCol::<i64>::from(vec![4, 5, 6]);
        i64s.extend_from_col_range(&tmp, 0..3)?;
        tmp.add_null()?;
        i64s.extend_from_col_range(&tmp, 0..4)?;
        assert_eq!(10, i64s.len());
        i64s.extend_from_col_range(&tmp, 0..4)?;
        assert_eq!(14, i64s.len());
        Ok(())
    }

    #[test]
    fn test_sized_col_conv() {
        let i64s = VecSizedCol::from(vec![1, 2, 3]);
        assert_eq!(vec![1, 2, 3], i64s.into_data());
    }
}
