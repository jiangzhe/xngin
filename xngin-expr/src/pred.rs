use crate::Expr;

/// Predicate represents filter conditions in conjunctive form.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pred {
    True,
    False,
    Conj(Vec<Expr>),
    Disj(Vec<Expr>),
    Xor(Vec<Expr>),
    // Wrapped expression must return bool.
    // It can be achieved by adding cast function.
    Func(PredFunc),
    Not(Box<Expr>),
    InSubquery(Box<Expr>, Box<Expr>),
    NotInSubquery(Box<Expr>, Box<Expr>),
    Exists(Box<Expr>),
    NotExists(Box<Expr>),
}

impl Pred {
    #[inline]
    pub fn func(kind: PredFuncKind, args: Vec<Expr>) -> Self {
        Pred::Func(PredFunc {
            kind,
            args: args.into_boxed_slice(),
        })
    }

    /// abbr for cast
    #[inline]
    pub fn cast(arg: Expr) -> Self {
        Pred::func(PredFuncKind::Cast, vec![arg])
    }
}

/// Predicate function is special function that returns only bool value.
/// It is separated from common function because in SQL context, predicates
/// are very important for optimization and we'd like to specially handle
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PredFunc {
    pub kind: PredFuncKind,
    pub args: Box<[Expr]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredFuncKind {
    // cast
    Cast,
    // comparison
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    NotEqual,
    // special comparison: is
    IsNull,
    IsNotNull,
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    // special comparison: match
    SafeEqual,
    Like,
    NotLike,
    Regexp,
    NotRegexp,
    // in values
    // layout of arguments: first arg is lhs, others are in list.
    InValues,
    NotInValues,
    Between,
    NotBetween,
}

impl PredFuncKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            PredFuncKind::Cast => "cast",
            PredFuncKind::Equal => "eq",
            PredFuncKind::Greater => "gt",
            PredFuncKind::GreaterEqual => "ge",
            PredFuncKind::Less => "lt",
            PredFuncKind::LessEqual => "le",
            PredFuncKind::NotEqual => "ne",
            PredFuncKind::IsNull => "isnull",
            PredFuncKind::IsNotNull => "isnotnull",
            PredFuncKind::IsTrue => "istrue",
            PredFuncKind::IsNotTrue => "isnottrue",
            PredFuncKind::IsFalse => "isfalse",
            PredFuncKind::IsNotFalse => "isnotfalse",
            PredFuncKind::SafeEqual => "safeeq",
            PredFuncKind::Like => "like",
            PredFuncKind::NotLike => "notlike",
            PredFuncKind::Regexp => "regexp",
            PredFuncKind::NotRegexp => "notregexp",
            PredFuncKind::InValues => "in",
            PredFuncKind::NotInValues => "notin",
            PredFuncKind::Between => "between",
            PredFuncKind::NotBetween => "notbetween",
        }
    }
}
