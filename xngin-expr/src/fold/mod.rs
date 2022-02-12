mod add;
mod cmp;
mod neg;
mod not;
mod sub;

use crate::error::Result;
use crate::{Const, Expr, Func, FuncKind, Pred, PredFunc, PredFuncKind};

pub use add::*;
pub use cmp::*;
pub use neg::*;
pub use not::*;
use std::borrow::Cow;
pub use sub::*;

/// General trait to wrap expressions to perform constant folding,
/// as well as checking whether an expression rejects null given
/// specific condition.
pub trait ConstFold: Sized {
    /// fold consumes self and returns any error if folding can
    /// be performed but fails.
    fn fold(&self) -> Result<Option<Const>> {
        self.transform_fold(|e| Cow::Borrowed(e))
    }

    fn transform_fold<T: Fn(&Expr) -> Cow<'_, Expr>>(&self, transform: T) -> Result<Option<Const>>;

    fn reject_null<T: Fn(&Expr) -> Cow<'_, Expr>>(&self, transform: T) -> Result<bool> {
        self.transform_fold(transform).map(|res| match res {
            Some(Const::Null) => true,
            Some(c) => c.is_zero().unwrap_or_default(),
            _ => false,
        })
    }
}

impl ConstFold for Func {
    fn transform_fold<T: Fn(&Expr) -> Cow<'_, Expr>>(&self, transform: T) -> Result<Option<Const>> {
        match self.kind {
            FuncKind::Neg => {
                let arg = transform(&self.args[0]);
                fold_neg(&arg)
            }
            FuncKind::Add => {
                let lhs = transform(&self.args[0]);
                let rhs = transform(&self.args[1]);
                fold_add(&lhs, &rhs)
            }
            FuncKind::Sub => {
                let lhs = transform(&self.args[0]);
                let rhs = transform(&self.args[1]);
                fold_sub(&lhs, &rhs)
            }
            _ => Ok(None),
        }
    }
}

impl ConstFold for Pred {
    fn transform_fold<T: Fn(&Expr) -> Cow<'_, Expr>>(&self, transform: T) -> Result<Option<Const>> {
        match self {
            Pred::Not(e) => {
                let arg = transform(e);
                fold_not(&arg)
            }
            Pred::Func(PredFunc { kind, args }) => match kind {
                PredFuncKind::Equal => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_eq(&lhs, &rhs)
                }
                PredFuncKind::Greater => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_gt(&lhs, &rhs)
                }
                PredFuncKind::GreaterEqual => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_ge(&lhs, &rhs)
                }
                PredFuncKind::Less => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_lt(&lhs, &rhs)
                }
                PredFuncKind::LessEqual => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_le(&lhs, &rhs)
                }
                PredFuncKind::NotEqual => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_ne(&lhs, &rhs)
                }
                PredFuncKind::SafeEqual => {
                    let lhs = transform(&args[0]);
                    let rhs = transform(&args[1]);
                    fold_safeeq(&lhs, &rhs)
                }
                PredFuncKind::IsNull => {
                    let arg = transform(&args[0]);
                    fold_isnull(&arg)
                }
                PredFuncKind::IsNotNull => {
                    let arg = transform(&args[0]);
                    fold_isnotnull(&arg)
                }
                PredFuncKind::IsTrue => {
                    let arg = transform(&args[0]);
                    fold_istrue(&arg)
                }
                PredFuncKind::IsNotTrue => {
                    let arg = transform(&args[0]);
                    fold_isnottrue(&arg)
                }
                PredFuncKind::IsFalse => {
                    let arg = transform(&args[0]);
                    fold_isfalse(&arg)
                }
                PredFuncKind::IsNotFalse => {
                    let arg = transform(&args[0]);
                    fold_isnotfalse(&arg)
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::Const::{self, *};
    use std::sync::Arc;

    pub(crate) fn new_f64(v: f64) -> Const {
        Const::new_f64(v).unwrap()
    }

    pub(crate) fn new_decimal(s: &str) -> Const {
        let d: xngin_datatype::Decimal = s.parse().unwrap();
        Decimal(d)
    }

    pub(crate) fn new_str(s: &str) -> Const {
        Const::String(Arc::from(s))
    }

    pub(crate) fn new_bytes(bs: &[u8]) -> Const {
        Const::Bytes(Arc::from(bs))
    }
}
