use crate::error::{Error, Result};
use std::sync::Arc;
use xngin_common::array::Array;
use xngin_common::bitmap::Bitmap;
use xngin_common::repr::ByteRepr;
use xngin_datatype::PreciseType;
use xngin_storage::attr::Attr;
use xngin_storage::codec::{Codec, Single};

/// Evaluation of binary expression.
pub trait BinaryEval {
    fn binary_eval(&self, res_ty: PreciseType, lhs: &Attr, rhs: &Attr) -> Result<Attr>;
}

/// Kinds of arithmetic expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithKind {
    Add,
    Sub,
    Mul,
    Div,
}

impl ArithKind {
    #[inline]
    pub fn eval(&self, lhs: &Attr, rhs: &Attr) -> Result<Attr> {
        match (self, lhs.ty, rhs.ty) {
            (ArithKind::Add, PreciseType::Int(4, l_unsigned), PreciseType::Int(4, r_unsigned)) => {
                AddI32.binary_eval(PreciseType::Int(4, l_unsigned | r_unsigned), lhs, rhs)
            }
            (ArithKind::Add, PreciseType::Int(8, l_unsigned), PreciseType::Int(8, r_unsigned)) => {
                AddI64.binary_eval(PreciseType::Int(8, l_unsigned | r_unsigned), lhs, rhs)
            }
            _ => Err(Error::UnsupportedArithOp),
        }
    }
}

macro_rules! impl_arith_eval {
    ($id:ident, $l:ty, $r:ty, $o:ty, $op:tt) => {
        pub struct $id;
        impl ArithEval for $id {
            type L = $l;
            type R = $r;
            type O = $o;
            #[inline]
            fn apply(&self, lhs: Self::L, rhs: Self::R) -> Self::O {
                lhs $op rhs
            }
        }
    }
}

/// Template trait for arithmetic expression evaluation
pub trait ArithEval {
    type L: ByteRepr + Copy;
    type R: ByteRepr + Copy;
    type O: ByteRepr + Copy;

    fn apply(&self, lhs: Self::L, rhs: Self::R) -> Self::O;

    /// Apply calculation of single and single.
    #[inline]
    fn apply_both_single(&self, res_ty: PreciseType, l: &Single, r: &Single) -> Result<Attr> {
        let (l_valid, l_val) = l.view();
        let (r_valid, r_val) = r.view();
        if l_valid && r_valid {
            let res = self.apply(l_val, r_val);
            Ok(Attr::new_single(res_ty, Single::new(res, l.len)))
        } else {
            Ok(Attr::new_single(res_ty, Single::new_null(l.len)))
        }
    }

    /// Apply calculation of array and single.
    #[inline]
    fn apply_array_single(
        &self,
        res_ty: PreciseType,
        l_vmap: Option<&Arc<Bitmap>>,
        l_vals: &[Self::L],
        r: &Single,
    ) -> Result<Attr> {
        let (r_valid, r_val) = r.view();
        if r_valid {
            let mut arr = Array::new_owned::<Self::O>(l_vals.len());
            let res_vals = arr.cast_slice_mut(l_vals.len()).unwrap();
            for (lhs, res) in l_vals.iter().zip(res_vals) {
                *res = self.apply(*lhs, r_val);
            }
            let validity = l_vmap.map(Bitmap::clone_to_owned);
            Ok(Attr::new_array(res_ty, arr, validity, None))
        } else {
            Ok(Attr::new_single(res_ty, Single::new_null(l_vals.len())))
        }
    }

    /// Apply calculation of single and array.
    #[inline]
    fn apply_single_array(
        &self,
        res_ty: PreciseType,
        l: &Single,
        r_vmap: Option<&Arc<Bitmap>>,
        r_vals: &[Self::R],
    ) -> Result<Attr> {
        let (l_valid, l_val) = l.view();
        if l_valid {
            let mut arr = Array::new_owned::<Self::O>(r_vals.len());
            let res_vals = arr.cast_slice_mut(r_vals.len()).unwrap();
            for (rhs, res) in r_vals.iter().zip(res_vals) {
                *res = self.apply(l_val, *rhs);
            }
            let validity = r_vmap.map(Bitmap::clone_to_owned);
            let codec = Codec::new_array(arr);
            Ok(Attr {
                ty: res_ty,
                validity,
                sma: None,
                codec,
            })
        } else {
            Ok(Attr::new_single(res_ty, Single::new_null(l.len)))
        }
    }

    /// Apply calculation of array and array.
    #[inline]
    fn apply_both_array(
        &self,
        res_ty: PreciseType,
        l_vmap: Option<&Arc<Bitmap>>,
        l_vals: &[Self::L],
        r_vmap: Option<&Arc<Bitmap>>,
        r_vals: &[Self::R],
    ) -> Result<Attr> {
        assert!(l_vals.len() == r_vals.len());
        let mut arr = Array::new_owned::<Self::O>(l_vals.len());
        let res_vals = arr.cast_slice_mut(l_vals.len()).unwrap();
        for ((lhs, rhs), res) in l_vals.iter().zip(r_vals).zip(res_vals) {
            *res = self.apply(*lhs, *rhs);
        }
        let validity = match (l_vmap, r_vmap) {
            (None, None) => None,
            (Some(l_bm), None) => Some(Bitmap::clone_to_owned(l_bm)),
            (None, Some(r_bm)) => Some(Bitmap::clone_to_owned(r_bm)),
            (Some(l_bm), Some(r_bm)) => {
                let mut res = Bitmap::to_owned(l_bm.as_ref());
                res.merge(r_bm)?;
                Some(Arc::new(res))
            }
        };
        let codec = Codec::new_array(arr);
        Ok(Attr {
            ty: res_ty,
            validity,
            sma: None,
            codec,
        })
    }
}

impl<F: ArithEval> BinaryEval for F {
    #[inline]
    fn binary_eval(&self, res_ty: PreciseType, lhs: &Attr, rhs: &Attr) -> Result<Attr> {
        if lhs.n_records() != rhs.n_records() {
            return Err(Error::RowNumberMismatch);
        }
        match (&lhs.codec, &rhs.codec) {
            (Codec::Single(l), Codec::Single(r)) => self.apply_both_single(res_ty, l, r),
            (Codec::Array(l), Codec::Single(r)) => {
                self.apply_array_single(res_ty, lhs.validity.as_ref(), l.cast_slice(), r)
            }
            (Codec::Single(l), Codec::Array(r)) => {
                self.apply_single_array(res_ty, l, rhs.validity.as_ref(), r.cast_slice())
            }
            (Codec::Array(l), Codec::Array(r)) => self.apply_both_array(
                res_ty,
                lhs.validity.as_ref(),
                l.cast_slice(),
                rhs.validity.as_ref(),
                r.cast_slice(),
            ),
            (Codec::Empty, _) | (_, Codec::Empty) => Ok(Attr::empty(res_ty)),
            // Binary expression does not support bitmap codec.
            (Codec::Bitmap(_), _) | (_, Codec::Bitmap(_)) => unreachable!(),
        }
    }
}

impl_arith_eval!(AddI32, i32, i32, i32, +);
impl_arith_eval!(AddI64, i64, i64, i64, +);
// impl_arith_eval!(SubI32, i32, i32, i32, -);
// impl_arith_eval!(SubI64, i64, i64, i64, -);

#[cfg(test)]
mod tests {
    use super::*;
    use xngin_storage::codec::Single;

    #[test]
    fn test_vec_eval_4096() {
        const SIZE: i32 = 4096;
        let c1 = Attr::from((0..SIZE as i32).into_iter());
        let c2 = Attr::from((0..SIZE as i32).into_iter());
        let add = AddI32;
        let res = add.binary_eval(PreciseType::i32(), &c1, &c2).unwrap();
        assert_eq!(4096, res.n_records());
    }

    #[test]
    fn test_vec_eval_add_i32() {
        let size = 10i32;
        let c1 = Attr::from((0..size).into_iter());
        let c2 = Attr::from((0..size).into_iter());
        let add = AddI32;
        let res = add.binary_eval(PreciseType::i32(), &c1, &c2).unwrap();
        match &res.codec {
            Codec::Array(array) => {
                let i32s = array.cast_slice::<i32>();
                let expected: Vec<_> = (0..size).map(|i| i + i).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let c3 = Attr::new_single(PreciseType::i32(), Single::new(1i32, size as usize));
        let c4 = Attr::new_single(PreciseType::i32(), Single::new(1i32, size as usize));
        let res = add.binary_eval(PreciseType::i32(), &c3, &c4).unwrap();
        match &res.codec {
            Codec::Single(single) => {
                let (valid, value) = single.view::<i32>();
                assert!(valid);
                assert!(value == 2);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(PreciseType::i32(), &c1, &c3).unwrap();
        match &res.codec {
            Codec::Array(array) => {
                let i32s = array.cast_slice::<i32>();
                let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(PreciseType::i32(), &c4, &c2).unwrap();
        match &res.codec {
            Codec::Array(array) => {
                let i32s = array.cast_slice::<i32>();
                let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let c5 = Attr::new_single(PreciseType::i32(), Single::new_null(size as usize));
        let res = add.binary_eval(PreciseType::i32(), &c1, &c5).unwrap();
        match &res.codec {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(PreciseType::i32(), &c2, &c5).unwrap();
        match &res.codec {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(PreciseType::i32(), &c5, &c2).unwrap();
        match &res.codec {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(PreciseType::i32(), &c5, &c1).unwrap();
        match &res.codec {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
    }
}
