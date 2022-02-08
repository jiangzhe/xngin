use crate::error::Result;
use crate::fold::ConstFold;
use crate::Const;

pub struct FoldNot<'a>(pub &'a Const);

impl ConstFold for FoldNot<'_> {
    fn fold(self) -> Result<Option<Const>> {
        let res = match self.0 {
            Const::Null => Some(Const::Null),
            other => other.is_zero().map(Const::Bool),
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::tests::{new_decimal, new_f64};
    use crate::Const::*;

    #[test]
    fn test_fold_not() {
        assert_eq_fold_not(I64(1), Bool(false));
        assert_eq_fold_not(I64(0), Bool(true));
        assert_eq_fold_not(U64(1), Bool(false));
        assert_eq_fold_not(U64(0), Bool(true));
        assert_eq_fold_not(new_f64(1.0), Bool(false));
        assert_eq_fold_not(new_f64(0.0), Bool(true));
        assert_eq_fold_not(new_decimal("1.0"), Bool(false));
        assert_eq_fold_not(new_decimal("0.0"), Bool(true));
        assert_eq_fold_not(Bool(true), Bool(false));
        assert_eq_fold_not(Bool(false), Bool(true));
        assert_eq_fold_not(Null, Null);
    }

    fn assert_eq_fold_not(c1: Const, c2: Const) {
        let res = FoldNot(&c1).fold().unwrap().unwrap();
        assert_eq!(res, c2)
    }
}
