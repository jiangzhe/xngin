use crate::col::sized::VecSizedCol;
use crate::col::BaseCol;
use xngin_datatype::DataType;

pub type VecI64Col = VecSizedCol<i64>;
pub type VecF64Col = VecSizedCol<f64>;

#[derive(Debug, Clone)]
pub enum TypedCol {
    VecI64(VecI64Col),
    VecF64(VecF64Col),
}

impl From<VecI64Col> for TypedCol {
    fn from(src: VecI64Col) -> Self {
        TypedCol::VecI64(src)
    }
}

impl From<VecF64Col> for TypedCol {
    fn from(src: VecF64Col) -> Self {
        TypedCol::VecF64(src)
    }
}

impl TypedCol {
    #[inline]
    pub fn new_i64s() -> Self {
        VecI64Col::new().into()
    }

    #[inline]
    pub fn new_i64s_with_capacity(capacity: usize) -> Self {
        VecI64Col::with_capacity(capacity).into()
    }

    #[inline]
    pub fn i64s(&self) -> Option<&VecI64Col> {
        match self {
            TypedCol::VecI64(vec) => Some(vec),
            _ => None,
        }
    }

    #[inline]
    pub fn i64s_mut(&mut self) -> Option<&mut VecI64Col> {
        match self {
            TypedCol::VecI64(vec) => Some(vec),
            _ => None,
        }
    }

    #[inline]
    pub fn new_f64s() -> Self {
        VecF64Col::new().into()
    }

    #[inline]
    pub fn new_f64s_with_capacity(capacity: usize) -> Self {
        VecF64Col::with_capacity(capacity).into()
    }

    #[inline]
    pub fn f64s(&self) -> Option<&VecF64Col> {
        match self {
            TypedCol::VecF64(vec) => Some(vec),
            _ => None,
        }
    }

    #[inline]
    pub fn f64s_mut(&mut self) -> Option<&mut VecF64Col> {
        match self {
            TypedCol::VecF64(vec) => Some(vec),
            _ => None,
        }
    }
}

impl BaseCol for TypedCol {
    fn len(&self) -> usize {
        match self {
            TypedCol::VecI64(v) => v.len(),
            TypedCol::VecF64(v) => v.len(),
        }
    }

    fn is_fixed_len(&self) -> bool {
        match self {
            TypedCol::VecI64(v) => v.is_fixed_len(),
            TypedCol::VecF64(v) => v.is_fixed_len(),
        }
    }

    fn data_ty(&self) -> DataType {
        match self {
            TypedCol::VecI64(v) => v.data_ty(),
            TypedCol::VecF64(v) => v.data_ty(),
        }
    }

    fn is_valid(&self, idx: usize) -> crate::error::Result<bool> {
        match self {
            TypedCol::VecI64(v) => v.is_valid(idx),
            TypedCol::VecF64(v) => v.is_valid(idx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BaseCol, DataType, TypedCol};

    #[test]
    fn test_typed_col() {
        let mut col = TypedCol::new_i64s();
        assert!(col.is_empty());
        assert_eq!(0, col.len());
        assert!(col.i64s().is_some());
        assert!(col.i64s_mut().is_some());
        assert_eq!(DataType::I64, col.data_ty());
        assert!(col.is_fixed_len());
        assert!(!col.is_var_len());
        assert!(col.is_valid(0).is_err());
        assert!(col.is_null(0).is_err());
        let _ = TypedCol::new_i64s_with_capacity(1024);
        let mut col = TypedCol::new_f64s();
        assert!(col.is_empty());
        assert_eq!(0, col.len());
        assert!(col.f64s().is_some());
        assert!(col.f64s_mut().is_some());
        assert_eq!(DataType::F64, col.data_ty());
        assert!(col.is_fixed_len());
        assert!(!col.is_var_len());
        assert!(col.is_valid(0).is_err());
        assert!(col.is_null(0).is_err());
        let _ = TypedCol::new_f64s_with_capacity(1024);
    }
}
