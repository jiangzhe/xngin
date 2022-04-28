use crate::error::{Error, Result};
use crate::BinaryEval;
use std::sync::Arc;
use xngin_common::bitmap::Bitmap;
use xngin_common::repr::ByteRepr;
use xngin_datatype::PreciseType;
use xngin_storage::attr::Attr;
use xngin_storage::codec::{Codec, Single};

/// Kinds of comparison expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpKind {
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    NotEqual,
}

impl CmpKind {
    #[inline]
    pub fn eval(&self, lhs: &Attr, rhs: &Attr) -> Result<Attr> {
        match (self, lhs.ty, rhs.ty) {
            (CmpKind::Equal, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(EqualI32).binary_eval(PreciseType::bool(), lhs, rhs)
            }
            _ => Err(Error::UnsupportedArithOp),
        }
    }
}

pub trait CmpEval {
    type L: ByteRepr + Copy;
    type R: ByteRepr + Copy;

    /// Apply comparison between pair of element.
    fn apply_bool(&self, lhs: Self::L, rhs: Self::R) -> bool;

    /// Apply comparison between left chunk and right singleton.
    #[inline]
    fn apply_left_chunk(&self, lhs: &[Self::L], rhs: Self::R) -> u64 {
        let mut res = 0u64;
        let mut mask = 1;
        for l_val in lhs {
            let flag = self.apply_bool(*l_val, rhs);
            res |= if flag { mask } else { 0 };
            mask <<= 1;
        }
        res
    }

    /// Apply comparison between left singleton and right chunk.
    #[inline]
    fn apply_right_chunk(&self, lhs: Self::L, rhs: &[Self::R]) -> u64 {
        let mut res = 0u64;
        let mut mask = 1;
        for r_val in rhs {
            let flag = self.apply_bool(lhs, *r_val);
            res |= if flag { mask } else { 0 };
            mask <<= 1;
        }
        res
    }

    /// Apply comparison between chunk and chunk, returns packed u64.
    #[inline]
    fn apply_chunk(&self, lhs: &[Self::L], rhs: &[Self::R]) -> u64 {
        let mut res = 0u64;
        let mut mask = 1;
        for (l_val, r_val) in lhs.iter().zip(rhs) {
            let flag = self.apply_bool(*l_val, *r_val);
            res |= if flag { mask } else { 0 };
            mask <<= 1;
        }
        res
    }

    /// Apply calculation of single and single.
    #[inline]
    fn apply_both_single(&self, res_ty: PreciseType, l: &Single, r: &Single) -> Result<Attr> {
        let (l_valid, l_val) = l.view();
        let (r_valid, r_val) = r.view();
        if l_valid && r_valid {
            let res = self.apply_bool(l_val, r_val);
            Ok(Attr::new_single(res_ty, Single::new_bool(res, l.len)))
        } else {
            Ok(Attr::new_single(res_ty, Single::new_null(l.len)))
        }
    }

    /// Apply calculation of array and single.
    #[inline]
    fn apply_array_single(
        &self,
        l_vmap: Option<&Arc<Bitmap>>,
        l_vals: &[Self::L],
        r: &Single,
    ) -> Result<Attr> {
        let (r_valid, r_val) = r.view();
        let len = l_vals.len();
        if r_valid {
            let mut res = Bitmap::with_len(len);
            let (res_u64s, _) = res.u64s_mut();
            for (lhs, res) in l_vals.chunks(64).zip(res_u64s) {
                *res = self.apply_left_chunk(lhs, r_val);
            }
            let validity = l_vmap.map(Bitmap::clone_to_owned);
            Ok(Attr::new_bitmap(res, validity))
        } else {
            Ok(Attr::new_single(
                PreciseType::bool(),
                Single::new_null(l_vals.len()),
            ))
        }
    }

    /// Apply calculation of single and array
    #[inline]
    fn apply_single_array(
        &self,
        l: &Single,
        r_vmap: Option<&Arc<Bitmap>>,
        r_vals: &[Self::R],
    ) -> Result<Attr> {
        let (l_valid, l_val) = l.view();
        if l_valid {
            let mut res = Bitmap::with_len(r_vals.len());
            let (res_u64s, _) = res.u64s_mut();
            for (rhs, res) in r_vals.chunks(64).zip(res_u64s) {
                *res = self.apply_right_chunk(l_val, rhs);
            }
            let validity = r_vmap.map(Bitmap::clone_to_owned);
            Ok(Attr::new_bitmap(res, validity))
        } else {
            Ok(Attr::new_single(
                PreciseType::bool(),
                Single::new_null(r_vals.len()),
            ))
        }
    }

    /// Apply calculation of array and array.
    #[inline]
    fn apply_both_array(
        &self,
        l_vmap: Option<&Arc<Bitmap>>,
        l_vals: &[Self::L],
        r_vmap: Option<&Arc<Bitmap>>,
        r_vals: &[Self::R],
    ) -> Result<Attr> {
        assert!(l_vals.len() == r_vals.len());
        let mut res = Bitmap::with_len(r_vals.len());
        let (res_u64s, _) = res.u64s_mut();
        for ((lhs, rhs), res) in l_vals.chunks(64).zip(r_vals.chunks(64)).zip(res_u64s) {
            *res = self.apply_chunk(lhs, rhs);
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
        Ok(Attr::new_bitmap(res, validity))
    }
}

struct Impl<T>(T);

impl<T: CmpEval> BinaryEval for Impl<T> {
    #[inline]
    fn binary_eval(&self, res_ty: PreciseType, lhs: &Attr, rhs: &Attr) -> Result<Attr> {
        debug_assert_eq!(PreciseType::bool(), res_ty);
        if lhs.n_records() != rhs.n_records() {
            return Err(Error::RowNumberMismatch);
        }
        match (&lhs.codec, &rhs.codec) {
            (Codec::Single(l), Codec::Single(r)) => self.0.apply_both_single(res_ty, l, r),
            (Codec::Array(l), Codec::Single(r)) => {
                self.0
                    .apply_array_single(lhs.validity.as_ref(), l.cast_slice(), r)
            }
            (Codec::Single(l), Codec::Array(r)) => {
                self.0
                    .apply_single_array(l, rhs.validity.as_ref(), r.cast_slice())
            }
            (Codec::Array(l), Codec::Array(r)) => self.0.apply_both_array(
                lhs.validity.as_ref(),
                l.cast_slice(),
                rhs.validity.as_ref(),
                r.cast_slice(),
            ),
            (Codec::Empty, _) | (_, Codec::Empty) => Ok(Attr::empty(res_ty)),
            // Arithmetic expression does not support bitmap codec.
            (Codec::Bitmap(_), _) | (_, Codec::Bitmap(_)) => Err(Error::UnsupportedArithOp),
        }
    }
}

macro_rules! impl_cmp_eval_for_iso_num {
    ($id:ident, $ty:ty, $op:tt) => {
        pub struct $id;
        impl CmpEval for $id {
            type L = $ty;
            type R = $ty;

            #[inline]
            fn apply_bool(&self, lhs: Self::L, rhs: Self::R) -> bool {
                lhs $op rhs
            }
        }
    }
}

impl_cmp_eval_for_iso_num!(EqualI32, i32, ==);
impl_cmp_eval_for_iso_num!(GreaterI32, i32, >);
impl_cmp_eval_for_iso_num!(GreaterEqualI32, i32, >=);
impl_cmp_eval_for_iso_num!(LessI32, i32, <);
impl_cmp_eval_for_iso_num!(LessEqualI32, i32, <=);
impl_cmp_eval_for_iso_num!(NotEqualI32, i32, !=);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp_eval_eq_i32() {
        let size = 10i32;
        let c1 = Attr::from((0..size).into_iter());
        let c2 = Attr::from((0..size).into_iter());
        let c3 = Attr::new_single(PreciseType::i32(), Single::new(0i32, size as usize));
        let c4 = Attr::new_single(PreciseType::i32(), Single::new(0i32, size as usize));
        let c5 = Attr::new_single(PreciseType::i32(), Single::new_null(size as usize));
        let eq = CmpKind::Equal;
        // array vs array
        let res = eq.eval(&c1, &c2).unwrap();
        let res = res.codec.as_bitmap().unwrap();
        assert!(res.bools().all(|b| b));
        // single vs single
        let res = eq.eval(&c3, &c4).unwrap();
        let res = res.codec.as_single().unwrap();
        let (valid, value) = res.view::<u8>();
        assert!(valid);
        assert_eq!(1, value);
        // array vs single
        let res = eq.eval(&c1, &c3).unwrap();
        let res = res.codec.as_bitmap().unwrap();
        assert!(res.get(0).unwrap());
        assert!(res.bools().skip(1).all(|b| !b));
        // single vs array
        let res = eq.eval(&c4, &c2).unwrap();
        let res = res.codec.as_bitmap().unwrap();
        assert!(res.get(0).unwrap());
        assert!(res.bools().skip(1).all(|b| !b));
        // array vs null
        let res = eq.eval(&c1, &c5).unwrap();
        let res = res.codec.as_single().unwrap();
        assert!(!res.valid);
        // null vs array
        let res = eq.eval(&c5, &c1).unwrap();
        let res = res.codec.as_single().unwrap();
        let (valid, _) = res.view::<u8>();
        assert!(!valid);
        // single vs null
        let res = eq.eval(&c3, &c5).unwrap();
        let res = res.codec.as_single().unwrap();
        let (valid, _) = res.view::<u8>();
        assert!(!valid);
    }
}
