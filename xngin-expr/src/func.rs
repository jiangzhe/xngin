use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Func {
    pub kind: FuncKind,
    pub args: Box<[Expr]>,
}

impl Default for Func {
    fn default() -> Self {
        Func {
            kind: FuncKind::Uninit,
            args: Box::default(),
        }
    }
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
    // Uninitialized, should be used only for intermediate placeholder
    // in transformation
    Uninit,
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

impl FuncKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            FuncKind::Uninit => "uninit",
            FuncKind::Neg => "neg",
            FuncKind::BitInv => "bitinv",
            FuncKind::Add => "add",
            FuncKind::Sub => "sub",
            FuncKind::Mul => "mul",
            FuncKind::Div => "div",
            FuncKind::IntDiv => "intdiv",
            FuncKind::BitAnd => "bitand",
            FuncKind::BitOr => "bitor",
            FuncKind::BitXor => "bitxor",
            FuncKind::BitShl => "bitshl",
            FuncKind::BitShr => "bitshr",
            FuncKind::Case => "case",
            FuncKind::Extract => "extract",
            FuncKind::Substring => "substring",
        }
    }
}
