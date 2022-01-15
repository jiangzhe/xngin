use crate::Expr;

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
    /// Unary operators: 1 arg
    // Negate
    Neg,
    // Bit inverse
    BitInv,
    /// Binary operators: 2 args
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
    /// Case when clause.
    // arguments: [node, else, when, then, ..., when, then]
    Case,
    /// SQL builtin functions: variable args
    // extract: [time_unit, arg]
    Extract,
    // substring: [arg, start, end]
    Substring,
}
