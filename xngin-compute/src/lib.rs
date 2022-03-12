pub mod arith;
pub mod error;

use crate::error::Result;
use xngin_common::array::{ArrayBuild, VecArray};
use xngin_common::bitmap::{bitmap_merge, VecBitmap, WriteBitmap};
use xngin_storage::codec::{Codec, FlatCodec, SingleCodec};

/// Main trait for vectorized evaluation.
pub trait VecEval {
    type Input;
    fn vec_eval(&mut self, input: &Self::Input) -> Result<Codec>;
}

pub fn binary_eval_i32s<F: Fn(i32, i32) -> i32>(lhs: &Codec, rhs: &Codec, f: F) -> Result<Codec> {
    match (lhs, rhs) {
        (Codec::Single(l), Codec::Single(r)) => {
            let (l_valid, l_val) = l.view_i32();
            let (r_valid, r_val) = r.view_i32();
            if l_valid && r_valid {
                let res = f(l_val, r_val);
                Ok(Codec::Single(SingleCodec::new_i32(res)))
            } else {
                Ok(Codec::Single(SingleCodec::new_null()))
            }
        }
        (Codec::Flat(l), Codec::Single(r)) => {
            let (r_valid, r_val) = r.view_i32();
            if r_valid {
                let (l_vmap, l_vals) = l.view_i32s();
                let mut arr = VecArray::default();
                let res_vals = arr.build_i32s(l_vals.len());
                for (lhs, res) in l_vals.iter().zip(res_vals) {
                    *res = f(*lhs, r_val);
                }
                let res_vmap = l_vmap.map(|vm| VecBitmap::from((vm, l_vals.len())));
                Ok(Codec::Flat(FlatCodec::new_owned(res_vmap, arr)))
            } else {
                Ok(Codec::Single(SingleCodec::new_null()))
            }
        }
        (Codec::Single(l), Codec::Flat(r)) => {
            let (l_valid, l_val) = l.view_i32();
            if l_valid {
                let (r_vmap, r_vals) = r.view_i32s();
                let mut arr = VecArray::default();
                let res_vals = arr.build_i32s(r_vals.len());
                for (rhs, res) in r_vals.iter().zip(res_vals) {
                    *res = f(l_val, *rhs);
                }
                let res_vmap = r_vmap.map(|vm| VecBitmap::from((vm, r_vals.len())));
                Ok(Codec::Flat(FlatCodec::new_owned(res_vmap, arr)))
            } else {
                Ok(Codec::Single(SingleCodec::new_null()))
            }
        }
        (Codec::Flat(l), Codec::Flat(r)) => {
            let (l_vmap, l_vals) = l.view_i32s();
            let (r_vmap, r_vals) = r.view_i32s();
            assert!(l_vals.len() == r_vals.len());
            let mut arr = VecArray::default();
            let res_vals = arr.build_i32s(l_vals.len());
            for ((lhs, rhs), res) in l_vals.iter().zip(r_vals).zip(res_vals) {
                *res = f(*lhs, *rhs);
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
