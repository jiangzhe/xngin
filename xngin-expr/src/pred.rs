use crate::expr::Expr;

/// Predicate represents filter conditions in conjunctive form.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pred {
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
        use PredFuncKind::*;
        match self {
            Equal => "eq",
            Greater => "gt",
            GreaterEqual => "ge",
            Less => "lt",
            LessEqual => "le",
            NotEqual => "ne",
            IsNull => "isnull",
            IsNotNull => "isnotnull",
            IsTrue => "istrue",
            IsNotTrue => "isnottrue",
            IsFalse => "isfalse",
            IsNotFalse => "isnotfalse",
            SafeEqual => "safeeq",
            Like => "like",
            NotLike => "notlike",
            Regexp => "regexp",
            NotRegexp => "notregexp",
            InValues => "in",
            NotInValues => "notin",
            Between => "between",
            NotBetween => "notbetween",
        }
    }

    #[inline]
    pub fn flip(&self) -> Option<Self> {
        use PredFuncKind::*;
        let res = match self {
            Equal => NotEqual,
            Greater => LessEqual,
            GreaterEqual => Less,
            Less => GreaterEqual,
            LessEqual => Greater,
            NotEqual => Equal,
            IsNull => IsNotNull,
            IsNotNull => IsNull,
            IsTrue => IsNotTrue,
            IsNotTrue => IsTrue,
            IsFalse => IsNotFalse,
            IsNotFalse => IsFalse,
            SafeEqual => return None,
            Like => NotLike,
            NotLike => Like,
            Regexp => NotRegexp,
            NotRegexp => Regexp,
            InValues => NotInValues,
            NotInValues => InValues,
            Between => NotBetween,
            NotBetween => Between,
        };
        Some(res)
    }
}
