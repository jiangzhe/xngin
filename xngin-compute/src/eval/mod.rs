use crate::error::Result;
use xngin_expr::{Const, Expr, Func};
use xngin_storage::block::Block;
use xngin_storage::codec::{Codec, SingleCodec};

/// Core trait for expression evaluation.
pub trait BlockEval {
    fn block_eval(&self, input: &Block) -> Result<Codec>;
}

impl BlockEval for Expr {
    #[inline]
    fn block_eval(&self, input: &Block) -> Result<Codec> {
        match self {
            Expr::Const(c) => c.block_eval(input),
            Expr::Func(f) => f.block_eval(input),
            _ => todo!(),
        }
    }
}

impl BlockEval for Const {
    #[inline]
    fn block_eval(&self, input: &Block) -> Result<Codec> {
        let res = match self {
            Const::I64(i) => Codec::Single(SingleCodec::new(*i, input.len)),
            Const::Null => Codec::Single(SingleCodec::new_null(input.len)),
            _ => todo!(),
        };
        Ok(res)
    }
}

impl BlockEval for Func {
    #[inline]
    fn block_eval(&self, _input: &Block) -> Result<Codec> {
        todo!()
    }
}
