use crate::error::Result;
use crate::{binary_eval_i32s, VecEval};
use xngin_storage::codec::Codec;

pub struct AddI32;

impl VecEval for AddI32 {
    type Input = (Codec, Codec);
    fn vec_eval(&mut self, (lhs, rhs): &Self::Input) -> Result<Codec> {
        binary_eval_i32s(lhs, rhs, |l, r| l + r)
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
        let in12 = (c1, c2);
        let mut add = AddI32;
        let res = add.vec_eval(&in12).unwrap();
        match res {
            Codec::Flat(flat) => {
                let (_, i32s) = flat.view_i32s();
                let expected: Vec<_> = (0..size).map(|i| i + i).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let c3 = Codec::Single(SingleCodec::new_i32(1));
        let c4 = Codec::Single(SingleCodec::new_i32(1));
        let in34 = (c3, c4);
        let res = add.vec_eval(&in34).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, value) = single.view_i32();
                assert!(valid);
                assert!(value == 2);
            }
            _ => panic!("failed"),
        }
        let in13 = (in12.0, in34.0);
        let res = add.vec_eval(&in13).unwrap();
        match res {
            Codec::Flat(flat) => {
                let (_, i32s) = flat.view_i32s();
                let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let in42 = (in34.1, in12.1);
        let res = add.vec_eval(&in42).unwrap();
        match res {
            Codec::Flat(flat) => {
                let (_, i32s) = flat.view_i32s();
                let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
                assert_eq!(&expected, i32s);
            }
            _ => panic!("failed"),
        }
        let c5 = Codec::Single(SingleCodec::new_null());
        let in15 = (in13.0, c5);
        let res = add.vec_eval(&in15).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view_i32();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let in25 = (in42.1, in15.1);
        let res = add.vec_eval(&in25).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view_i32();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let in52 = (in25.1, in25.0);
        let res = add.vec_eval(&in52).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view_i32();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
        let in51 = (in52.0, in15.0);
        let res = add.vec_eval(&in51).unwrap();
        match res {
            Codec::Single(single) => {
                let (valid, _) = single.view_i32();
                assert!(!valid);
            }
            _ => panic!("failed"),
        }
    }
}
