pub mod arith;
pub mod cmp;
pub mod error;
pub mod eval;
pub mod logic;
pub mod sel;

use crate::error::Result;
use xngin_datatype::PreciseType;
use xngin_storage::attr::Attr;

/// Evaluation of binary expression.
pub trait BinaryEval {
    fn binary_eval(&self, res_ty: PreciseType, lhs: &Attr, rhs: &Attr) -> Result<Attr>;
}

/// Evaluation of unary expression.
pub trait UnaryEval {
    fn unary_eval(&self, res_ty: PreciseType, lhs: &Attr) -> Result<Attr>;
}
