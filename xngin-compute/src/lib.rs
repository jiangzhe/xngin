pub mod arith;
pub mod error;
pub mod eval;

use crate::error::Result;
use xngin_storage::codec::Codec;

/// Main trait for vectorized evaluation.
pub trait VecEval {
    type Input;
    fn vec_eval(&self, input: &Self::Input) -> Result<Codec>;
}

pub trait BinaryEval {
    fn binary_eval(&self, lhs: &Codec, rhs: &Codec) -> Result<Codec>;
}
