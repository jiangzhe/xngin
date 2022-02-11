use crate::error::Result;
use crate::fold::ConstFold;
use crate::{Const, Expr};
use std::cmp::Ordering;
use xngin_datatype::AlignPartialOrd;

macro_rules! impl_fold_cmp {
    ( $ty:ident, $($e:pat),* ) => {
        pub struct $ty<'a>(pub &'a Expr, pub &'a Expr);

        impl ConstFold for $ty<'_> {
            fn fold(self) -> Result<Option<Const>> {
                let res = match (self.0, self.1) {
                    (Expr::Const(Const::Null), _) => Some(Const::Null),
                    (_, Expr::Const(Const::Null)) => Some(Const::Null),
                    (Expr::Const(c1), Expr::Const(c2)) => match c1.align_partial_cmp(c2) {
                        None => None,
                        $(
                            Some($e) => Some(Const::Bool(true)),
                        )*
                        _ => Some(Const::Bool(false)),
                    }
                    _ => None,
                };
                Ok(res)
            }
        }
    }
}

impl_fold_cmp!(FoldEqual, Ordering::Equal);
impl_fold_cmp!(FoldGreater, Ordering::Greater);
impl_fold_cmp!(FoldGreaterEqual, Ordering::Greater, Ordering::Equal);
impl_fold_cmp!(FoldLess, Ordering::Less);
impl_fold_cmp!(FoldLessEqual, Ordering::Less, Ordering::Equal);
impl_fold_cmp!(FoldNotEqual, Ordering::Less, Ordering::Greater);

pub struct FoldSafeEqual<'a>(pub &'a Expr, pub &'a Expr);

impl ConstFold for FoldSafeEqual<'_> {
    fn fold(self) -> Result<Option<Const>> {
        let res = match (self.0, self.1) {
            (Expr::Const(Const::Null), Expr::Const(Const::Null)) => Some(Const::Bool(true)),
            (Expr::Const(Const::Null), Expr::Const(_)) => Some(Const::Bool(false)),
            (Expr::Const(_), Expr::Const(Const::Null)) => Some(Const::Bool(false)),
            (Expr::Const(c1), Expr::Const(c2)) => match c1.align_partial_cmp(c2) {
                None => None,
                Some(Ordering::Equal) => Some(Const::Bool(true)),
                _ => Some(Const::Bool(false)),
            },
            _ => None,
        };
        Ok(res)
    }
}

pub struct FoldIsNull<'a>(pub &'a Expr);

impl ConstFold for FoldIsNull<'_> {
    fn fold(self) -> Result<Option<Const>> {
        let res = match self.0 {
            Expr::Const(Const::Null) => Some(Const::Bool(true)),
            Expr::Const(_) => Some(Const::Bool(false)),
            _ => None,
        };
        Ok(res)
    }
}

pub struct FoldIsNotNull<'a>(pub &'a Expr);

impl ConstFold for FoldIsNotNull<'_> {
    fn fold(self) -> Result<Option<Const>> {
        let res = match self.0 {
            Expr::Const(Const::Null) => Some(Const::Bool(false)),
            Expr::Const(_) => Some(Const::Bool(true)),
            _ => None,
        };
        Ok(res)
    }
}

pub struct FoldIsTrue<'a>(pub &'a Expr);

impl ConstFold for FoldIsTrue<'_> {
    fn fold(self) -> Result<Option<Const>> {
        let res = match self.0 {
            Expr::Const(Const::Null) => Some(Const::Null),
            Expr::Const(c) => c.is_zero().map(|zero| Const::Bool(!zero)),
            _ => None,
        };
        Ok(res)
    }
}

pub struct FoldIsFalse<'a>(pub &'a Expr);

impl ConstFold for FoldIsFalse<'_> {
    fn fold(self) -> Result<Option<Const>> {
        let res = match self.0 {
            Expr::Const(Const::Null) => Some(Const::Null),
            Expr::Const(c) => c.is_zero().map(Const::Bool),
            _ => None,
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::tests::{new_bytes, new_decimal, new_f64, new_str};
    use crate::Const::*;
    use std::sync::Arc;

    macro_rules! assert_fold_cmp_null {
        ($e1:expr, $e2:expr) => {
            assert_eq_fold_eq($e1, $e2, Null);
            assert_eq_fold_gt($e1, $e2, Null);
            assert_eq_fold_ge($e1, $e2, Null);
            assert_eq_fold_lt($e1, $e2, Null);
            assert_eq_fold_le($e1, $e2, Null);
            assert_eq_fold_ne($e1, $e2, Null);
        };
    }

    #[test]
    fn test_fold_cmp_null() {
        assert_fold_cmp_null!(Null, I64(1));
        assert_fold_cmp_null!(Null, U64(1));
        assert_fold_cmp_null!(Null, new_f64(1.0));
        assert_fold_cmp_null!(Null, new_decimal("1.0"));
        assert_fold_cmp_null!(Null, Bool(true));
        assert_fold_cmp_null!(Null, String(Arc::from("abc")));
        assert_fold_cmp_null!(Null, Bytes(Arc::from("abc".as_bytes())));
        assert_fold_cmp_null!(I64(1), Null);
        assert_fold_cmp_null!(U64(1), Null);
        assert_fold_cmp_null!(new_f64(1.0), Null);
        assert_fold_cmp_null!(new_decimal("1.0"), Null);
        assert_fold_cmp_null!(Bool(true), Null);
        assert_fold_cmp_null!(new_str("abc"), Null);
        assert_fold_cmp_null!(new_bytes(b"abc"), Null);
        // todo: date, time, datetime
        assert_fold_cmp_null!(Null, Null);

        assert_eq_fold_se(Null, Null, Bool(true));
        assert_eq_fold_se(Null, I64(1), Bool(false));
        assert_eq_fold_se(I64(1), Null, Bool(false));
    }

    #[test]
    fn test_fold_cmp_non_null() {
        // i64
        assert_eq_fold_eq(I64(1), I64(1), Bool(true));
        assert_eq_fold_eq(I64(1), I64(-1), Bool(false));
        assert_eq_fold_gt(I64(1), I64(1), Bool(false));
        assert_eq_fold_gt(I64(1), I64(-1), Bool(true));
        assert_eq_fold_ge(I64(1), I64(1), Bool(true));
        assert_eq_fold_ge(I64(1), I64(-1), Bool(true));
        assert_eq_fold_ge(I64(-1), I64(1), Bool(false));
        assert_eq_fold_lt(I64(1), I64(1), Bool(false));
        assert_eq_fold_lt(I64(-1), I64(1), Bool(true));
        assert_eq_fold_le(I64(1), I64(1), Bool(true));
        assert_eq_fold_le(I64(-1), I64(1), Bool(true));
        assert_eq_fold_le(I64(1), I64(-1), Bool(false));
        assert_eq_fold_ne(I64(1), I64(1), Bool(false));
        assert_eq_fold_ne(I64(1), I64(-1), Bool(true));

        // u64
        assert_eq_fold_eq(U64(1), U64(1), Bool(true));
        assert_eq_fold_eq(U64(1), U64(0), Bool(false));
        assert_eq_fold_gt(U64(1), U64(1), Bool(false));
        assert_eq_fold_gt(U64(1), U64(0), Bool(true));
        assert_eq_fold_ge(U64(1), U64(1), Bool(true));
        assert_eq_fold_ge(U64(1), U64(0), Bool(true));
        assert_eq_fold_ge(U64(0), U64(1), Bool(false));
        assert_eq_fold_lt(U64(1), U64(1), Bool(false));
        assert_eq_fold_lt(U64(0), U64(1), Bool(true));
        assert_eq_fold_le(U64(1), U64(1), Bool(true));
        assert_eq_fold_le(U64(0), U64(1), Bool(true));
        assert_eq_fold_le(U64(1), U64(0), Bool(false));
        assert_eq_fold_ne(U64(1), U64(1), Bool(false));
        assert_eq_fold_ne(U64(1), U64(0), Bool(true));

        // f64
        assert_eq_fold_eq(new_f64(1.0), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_f64(0.0), Bool(false));
        assert_eq_fold_gt(new_f64(1.0), new_f64(1.0), Bool(false));
        assert_eq_fold_gt(new_f64(1.0), new_f64(0.0), Bool(true));
        assert_eq_fold_ge(new_f64(1.0), new_f64(1.0), Bool(true));
        assert_eq_fold_ge(new_f64(1.0), new_f64(0.0), Bool(true));
        assert_eq_fold_ge(new_f64(0.0), new_f64(1.0), Bool(false));
        assert_eq_fold_lt(new_f64(1.0), new_f64(1.0), Bool(false));
        assert_eq_fold_lt(new_f64(0.0), new_f64(1.0), Bool(true));
        assert_eq_fold_le(new_f64(1.0), new_f64(1.0), Bool(true));
        assert_eq_fold_le(new_f64(0.0), new_f64(1.0), Bool(true));
        assert_eq_fold_le(new_f64(1.0), new_f64(0.0), Bool(false));
        assert_eq_fold_ne(new_f64(1.0), new_f64(1.0), Bool(false));
        assert_eq_fold_ne(new_f64(1.0), new_f64(0.0), Bool(true));

        // decimal
        assert_eq_fold_eq(new_decimal("1.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_decimal("0.0"), Bool(false));
        assert_eq_fold_gt(new_decimal("1.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_gt(new_decimal("1.0"), new_decimal("0.0"), Bool(true));
        assert_eq_fold_ge(new_decimal("1.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_ge(new_decimal("1.0"), new_decimal("0.0"), Bool(true));
        assert_eq_fold_ge(new_decimal("0.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_lt(new_decimal("1.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_lt(new_decimal("0.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_le(new_decimal("1.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_le(new_decimal("0.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_le(new_decimal("1.0"), new_decimal("0.0"), Bool(false));
        assert_eq_fold_ne(new_decimal("1.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_ne(new_decimal("1.0"), new_decimal("0.0"), Bool(true));

        // bool
        assert_eq_fold_eq(Bool(true), Bool(true), Bool(true));
        assert_eq_fold_eq(Bool(true), Bool(false), Bool(false));
        assert_eq_fold_gt(Bool(true), Bool(true), Bool(false));
        assert_eq_fold_gt(Bool(true), Bool(false), Bool(true));
        assert_eq_fold_ge(Bool(true), Bool(true), Bool(true));
        assert_eq_fold_ge(Bool(true), Bool(false), Bool(true));
        assert_eq_fold_ge(Bool(false), Bool(true), Bool(false));
        assert_eq_fold_lt(Bool(true), Bool(true), Bool(false));
        assert_eq_fold_lt(Bool(false), Bool(true), Bool(true));
        assert_eq_fold_le(Bool(true), Bool(true), Bool(true));
        assert_eq_fold_le(Bool(false), Bool(true), Bool(true));
        assert_eq_fold_le(Bool(true), Bool(false), Bool(false));
        assert_eq_fold_ne(Bool(true), Bool(true), Bool(false));
        assert_eq_fold_ne(Bool(true), Bool(false), Bool(true));

        // string
        assert_eq_fold_eq(new_str("1"), new_str("1"), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_str("0"), Bool(false));
        assert_eq_fold_gt(new_str("1"), new_str("1"), Bool(false));
        assert_eq_fold_gt(new_str("1"), new_str("0"), Bool(true));
        assert_eq_fold_ge(new_str("1"), new_str("1"), Bool(true));
        assert_eq_fold_ge(new_str("1"), new_str("0"), Bool(true));
        assert_eq_fold_ge(new_str("0"), new_str("1"), Bool(false));
        assert_eq_fold_lt(new_str("1"), new_str("1"), Bool(false));
        assert_eq_fold_lt(new_str("0"), new_str("1"), Bool(true));
        assert_eq_fold_le(new_str("1"), new_str("1"), Bool(true));
        assert_eq_fold_le(new_str("0"), new_str("1"), Bool(true));
        assert_eq_fold_le(new_str("1"), new_str("0"), Bool(false));
        assert_eq_fold_ne(new_str("1"), new_str("1"), Bool(false));
        assert_eq_fold_ne(new_str("1"), new_str("0"), Bool(true));

        // bytes
        assert_eq_fold_eq(new_bytes(b"1"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_bytes(b"0"), Bool(false));
        assert_eq_fold_gt(new_bytes(b"1"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_gt(new_bytes(b"1"), new_bytes(b"0"), Bool(true));
        assert_eq_fold_ge(new_bytes(b"1"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_ge(new_bytes(b"1"), new_bytes(b"0"), Bool(true));
        assert_eq_fold_ge(new_bytes(b"0"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_lt(new_bytes(b"1"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_lt(new_bytes(b"0"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_le(new_bytes(b"1"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_le(new_bytes(b"0"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_le(new_bytes(b"1"), new_bytes(b"0"), Bool(false));
        assert_eq_fold_ne(new_bytes(b"1"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_ne(new_bytes(b"1"), new_bytes(b"0"), Bool(true));

        // todo: date, time, datetime
    }

    #[test]
    fn test_fold_cmp_align() {
        assert_eq_fold_eq(I64(1), U64(1), Bool(true));
        assert_eq_fold_eq(I64(1), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(I64(1), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(I64(1), Bool(true), Bool(true));
        assert_eq_fold_eq(I64(1), new_str("1"), Bool(true));
        assert_eq_fold_eq(I64(1), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(U64(1), I64(1), Bool(true));
        assert_eq_fold_eq(U64(1), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(U64(1), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(U64(1), Bool(true), Bool(true));
        assert_eq_fold_eq(U64(1), new_str("1"), Bool(true));
        assert_eq_fold_eq(U64(1), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_f64(1.0), I64(1), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), U64(1), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), Bool(true), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_str("1"), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_decimal("1.0"), I64(1), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), U64(1), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), Bool(true), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_str("1"), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(Bool(true), I64(1), Bool(true));
        assert_eq_fold_eq(Bool(true), U64(1), Bool(true));
        assert_eq_fold_eq(Bool(true), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(Bool(true), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(Bool(true), new_str("1"), Bool(true));
        assert_eq_fold_eq(Bool(true), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_str("1"), I64(1), Bool(true));
        assert_eq_fold_eq(new_str("1"), U64(1), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_str("1"), Bool(true), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_bytes(b"1"), I64(1), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), U64(1), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), Bool(true), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_str("1"), Bool(true));

        // todo: date, time, datetime
    }

    fn assert_eq_fold_eq(c1: Const, c2: Const, c3: Const) {
        let res = FoldEqual(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_gt(c1: Const, c2: Const, c3: Const) {
        let res = FoldGreater(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_ge(c1: Const, c2: Const, c3: Const) {
        let res = FoldGreaterEqual(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_lt(c1: Const, c2: Const, c3: Const) {
        let res = FoldLess(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_le(c1: Const, c2: Const, c3: Const) {
        let res = FoldLessEqual(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_ne(c1: Const, c2: Const, c3: Const) {
        let res = FoldNotEqual(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_se(c1: Const, c2: Const, c3: Const) {
        let res = FoldSafeEqual(&Expr::Const(c1), &Expr::Const(c2))
            .fold()
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }
}
