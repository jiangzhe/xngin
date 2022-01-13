use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Func {
    pub kind: FuncKind,
    pub args: Box<[Expr]>,
}

impl Func {
    #[inline]
    pub fn new(kind: FuncKind, args: Vec<Expr>) -> Self {
        Func {
            kind,
            args: args.into_boxed_slice(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FuncKind {
    // arithmetic unary
    Neg,
    BitInv,
    // arithmetic binary
    Add,
    Sub,
    Mul,
    Div,
    IntDiv,
    BitAnd,
    BitOr,
    BitXor,
    BitShl,
    BitShr,
}
