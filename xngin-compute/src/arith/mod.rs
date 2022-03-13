use crate::error::{Error, Result};
use crate::BinaryEval;
use xngin_common::array::{ArrayBuild, VecArray};
use xngin_common::bitmap::{bitmap_merge, VecBitmap, WriteBitmap};
use xngin_common::byte_repr::ByteRepr;
use xngin_storage::codec::{Codec, FlatCodec, SingleCodec};

// pub struct ArithFunc<F>(F);

pub trait ArithFunc {
    type L: ByteRepr + Copy;
    type R: ByteRepr + Copy;
    type O: ByteRepr + Copy;

    fn apply(&self, lhs: Self::L, rhs: Self::R) -> Self::O;
}

impl<F: ArithFunc> BinaryEval for F {
    #[inline]
    fn binary_eval(&self, lhs: &Codec, rhs: &Codec) -> Result<Codec> {
        if lhs.len() != rhs.len() {
            return Err(Error::RowNumberMismatch);
        }
        match (lhs, rhs) {
            (Codec::Single(l), Codec::Single(r)) => {
                let (l_valid, l_val) = l.view();
                let (r_valid, r_val) = r.view();
                if l_valid && r_valid {
                    let res = self.apply(l_val, r_val);
                    Ok(Codec::Single(SingleCodec::new(res, l.len())))
                } else {
                    Ok(Codec::Single(SingleCodec::new_null(l.len())))
                }
            }
            (Codec::Flat(l), Codec::Single(r)) => {
                let (r_valid, r_val) = r.view();
                if r_valid {
                    let (l_vmap, l_vals) = l.view();
                    let mut arr = VecArray::default();
                    let res_vals = arr.build(l_vals.len());
                    for (lhs, res) in l_vals.iter().zip(res_vals) {
                        *res = self.apply(*lhs, r_val);
                    }
                    let res_vmap = l_vmap.map(|vm| VecBitmap::from((vm, l_vals.len())));
                    Ok(Codec::Flat(FlatCodec::new_owned(res_vmap, arr)))
                } else {
                    Ok(Codec::Single(SingleCodec::new_null(l.len())))
                }
            }
            (Codec::Single(l), Codec::Flat(r)) => {
                let (l_valid, l_val) = l.view();
                if l_valid {
                    let (r_vmap, r_vals) = r.view();
                    let mut arr = VecArray::default();
                    let res_vals = arr.build(r_vals.len());
                    for (rhs, res) in r_vals.iter().zip(res_vals) {
                        *res = self.apply(l_val, *rhs);
                    }
                    let res_vmap = r_vmap.map(|vm| VecBitmap::from((vm, r_vals.len())));
                    Ok(Codec::Flat(FlatCodec::new_owned(res_vmap, arr)))
                } else {
                    Ok(Codec::Single(SingleCodec::new_null(l.len())))
                }
            }
            (Codec::Flat(l), Codec::Flat(r)) => {
                let (l_vmap, l_vals) = l.view();
                let (r_vmap, r_vals) = r.view();
                assert!(l_vals.len() == r_vals.len());
                let mut arr = VecArray::default();
                let res_vals = arr.build(l_vals.len());
                for ((lhs, rhs), res) in l_vals.iter().zip(r_vals).zip(res_vals) {
                    *res = self.apply(*lhs, *rhs);
                }
                let res_vmap = match (l_vmap, r_vmap) {
                    (None, None) => None,
                    (Some(l_bm), None) => Some(VecBitmap::from((l_bm, l_vals.len()))),
                    (None, Some(r_bm)) => Some(VecBitmap::from((r_bm, r_vals.len()))),
                    (Some(l_bm), Some(r_bm)) => {
                        let mut res = VecBitmap::from((l_bm, l_vals.len()));
                        let (res_bm, res_len) = res.aligned_mut();
                        bitmap_merge(res_bm, res_len, r_bm, r_vals.len());
                        Some(res)
                    }
                };
                Ok(Codec::Flat(FlatCodec::new_owned(res_vmap, arr)))
            }
        }
    }
}

pub struct AddI32;

impl ArithFunc for AddI32 {
    type L = i32;
    type R = i32;
    type O = i32;

    #[inline]
    fn apply(&self, lhs: i32, rhs: i32) -> i32 {
        lhs + rhs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xngin_storage::codec::{FlatCodec, OwnFlat, SingleCodec};

    #[test]
    fn test_vec_eval_add_i32() {
        let size = 10;
        let c1 = Codec::Flat(FlatCodec::Owned(OwnFlat::from_iter((0..size).into_iter())));
        let c2 = Codec::Flat(FlatCodec::Owned(OwnFlat::from_iter((0..size).into_iter())));
        let add = AddI32;
        let res = add.binary_eval(&c1, &c2).unwrap();
        match res {
            Codec::Flat(flat) => {
                let (_, i32s) = flat.view();
                let expected: Vec<_> = (0..size).map(|i| i + i).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let c3 = Codec::Single(SingleCodec::new(1, size as usize));
        let c4 = Codec::Single(SingleCodec::new(1, size as usize));
        let res = add.binary_eval(&c3, &c4).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, value) = single.view::<i32>();
                assert!(valid);
                assert!(value == 2);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(&c1, &c3).unwrap();
        match res {
            Codec::Flat(flat) => {
                let (_, i32s) = flat.view();
                let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(&c4, &c2).unwrap();
        match res {
            Codec::Flat(flat) => {
                let (_, i32s) = flat.view();
                let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let c5 = Codec::Single(SingleCodec::new_null(size as usize));
        let res = add.binary_eval(&c1, &c5).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(&c2, &c5).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(&c5, &c2).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let res = add.binary_eval(&c5, &c1).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view::<i32>();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
    }
}
