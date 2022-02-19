use crate::func::{Func, FuncKind};
use crate::pred::{Pred, PredFuncKind};
use smallvec::{smallvec, SmallVec};
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use xngin_catalog::TableID;
use xngin_datatype::{Date, Datetime, Decimal, Interval, Time, TimeUnit};

pub use xngin_datatype::{Const, ValidF64};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    Const(Const),
    Col(Col),
    Aggf(Aggf),
    Func(Func),
    Pred(Pred),
    Tuple(Vec<Expr>),
    /// Subquery that returns single value.
    Subq(SubqKind, QueryID),
    /// Placeholder can represent any intermediate value
    /// generated in building phase. It should be
    /// resolved as a normal expression later.
    /// Placeholder is also used in optimization, such as
    /// predicate pushdown.
    Plhd(Plhd),
    /// Predefined function argument.
    Farg(Farg),
}

impl Default for Expr {
    fn default() -> Self {
        Expr::Const(Const::Null)
    }
}

impl Expr {
    #[inline]
    pub fn table_col(table_id: TableID, idx: u32) -> Self {
        Expr::Col(Col::TableCol(table_id, idx))
    }

    #[inline]
    pub fn query_col(query_id: QueryID, idx: u32) -> Self {
        Expr::Col(Col::QueryCol(query_id, idx))
    }

    #[inline]
    pub fn correlated_col(query_id: QueryID, idx: u32) -> Self {
        Expr::Col(Col::CorrelatedCol(query_id, idx))
    }

    #[inline]
    pub fn func(kind: FuncKind, args: Vec<Expr>) -> Self {
        Expr::Func(Func::new(kind, args))
    }

    #[inline]
    pub fn pred_not(e: Expr) -> Self {
        Expr::Pred(Pred::Not(Box::new(e)))
    }

    #[inline]
    pub fn pred_in_subq(lhs: Expr, subq: Expr) -> Self {
        Expr::Pred(Pred::InSubquery(Box::new(lhs), Box::new(subq)))
    }

    #[inline]
    pub fn pred_not_in_subq(lhs: Expr, subq: Expr) -> Self {
        Expr::Pred(Pred::NotInSubquery(Box::new(lhs), Box::new(subq)))
    }

    #[inline]
    pub fn pred_exists(subq: Expr) -> Self {
        Expr::Pred(Pred::Exists(Box::new(subq)))
    }

    #[inline]
    pub fn pred_func(kind: PredFuncKind, args: Vec<Expr>) -> Self {
        Expr::Pred(Pred::func(kind, args))
    }

    #[inline]
    pub fn pred_conj(mut exprs: Vec<Expr>) -> Self {
        assert!(!exprs.is_empty());
        if exprs.len() == 1 {
            exprs.pop().unwrap()
        } else {
            Expr::pred_conj(exprs)
        }
    }

    #[inline]
    pub fn pred_and(lhs: Expr, rhs: Expr) -> Self {
        match (lhs, rhs) {
            (Expr::Pred(Pred::Conj(mut lhs)), Expr::Pred(Pred::Conj(rhs))) => {
                lhs.extend(rhs);
                Expr::pred_conj(lhs)
            }
            (Expr::Pred(Pred::Conj(mut lhs)), rhs) => {
                lhs.push(rhs);
                Expr::pred_conj(lhs)
            }
            (lhs, Expr::Pred(Pred::Conj(rhs))) => {
                let mut vs = Vec::with_capacity(rhs.len() + 1);
                vs.push(lhs);
                vs.extend(rhs);
                Expr::pred_conj(vs)
            }
            (lhs, rhs) => Expr::pred_conj(vec![lhs, rhs]),
        }
    }

    #[inline]
    pub fn const_null() -> Self {
        Expr::Const(Const::Null)
    }

    #[inline]
    pub fn const_bool(v: bool) -> Self {
        Expr::Const(Const::Bool(v))
    }

    #[inline]
    pub fn const_i64(i: i64) -> Self {
        Expr::Const(Const::I64(i))
    }

    #[inline]
    pub fn const_u64(u: u64) -> Self {
        Expr::Const(Const::U64(u))
    }

    /// Caller should ensure input is finite and is a number
    #[inline]
    pub fn const_f64(f: f64) -> Self {
        Expr::Const(Const::F64(ValidF64::new(f).unwrap()))
    }

    #[inline]
    pub fn const_decimal(d: Decimal) -> Self {
        Expr::Const(Const::Decimal(d))
    }

    #[inline]
    pub fn const_date(dt: Date) -> Self {
        Expr::Const(Const::Date(dt))
    }

    #[inline]
    pub fn const_time(tm: Time) -> Self {
        Expr::Const(Const::Time(tm))
    }

    #[inline]
    pub fn const_datetime(ts: Datetime) -> Self {
        Expr::Const(Const::Datetime(ts))
    }

    #[inline]
    pub fn const_interval(unit: TimeUnit, value: i32) -> Self {
        Expr::Const(Const::Interval(Interval { unit, value }))
    }

    #[inline]
    pub fn const_str(s: Arc<str>) -> Self {
        Expr::Const(Const::String(s))
    }

    #[inline]
    pub fn const_bytes(bs: Arc<[u8]>) -> Self {
        Expr::Const(Const::Bytes(bs))
    }

    #[inline]
    pub fn count_asterisk() -> Self {
        let af = Aggf {
            kind: AggKind::Count,
            q: Setq::All,
            arg: Box::new(Expr::const_i64(1)),
        };
        Expr::Aggf(af)
    }

    #[inline]
    pub fn count(q: Setq, expr: Expr) -> Self {
        let af = Aggf {
            kind: AggKind::Count,
            q,
            arg: Box::new(expr),
        };
        Expr::Aggf(af)
    }

    #[inline]
    pub fn sum(q: Setq, expr: Expr) -> Self {
        let af = Aggf {
            kind: AggKind::Sum,
            q,
            arg: Box::new(expr),
        };
        Expr::Aggf(af)
    }

    #[inline]
    pub fn avg(q: Setq, expr: Expr) -> Self {
        let af = Aggf {
            kind: AggKind::Avg,
            q,
            arg: Box::new(expr),
        };
        Expr::Aggf(af)
    }

    #[inline]
    pub fn min(q: Setq, expr: Expr) -> Self {
        let af = Aggf {
            kind: AggKind::Min,
            q,
            arg: Box::new(expr),
        };
        Expr::Aggf(af)
    }

    #[inline]
    pub fn max(q: Setq, expr: Expr) -> Self {
        let af = Aggf {
            kind: AggKind::Max,
            q,
            arg: Box::new(expr),
        };
        Expr::Aggf(af)
    }

    #[inline]
    pub fn ph_ident(uid: u32) -> Self {
        Expr::Plhd(Plhd::Ident(uid))
    }

    #[inline]
    pub fn ph_subquery(kind: SubqKind, uid: u32) -> Self {
        Expr::Plhd(Plhd::Subquery(kind, uid))
    }

    #[inline]
    pub fn farg_none() -> Self {
        Expr::Farg(Farg::None)
    }

    #[inline]
    pub fn is_const(&self) -> bool {
        matches!(self, Expr::Const(_))
    }

    #[inline]
    pub fn into_conj(self) -> Vec<Expr> {
        match self {
            Expr::Pred(Pred::Conj(es)) => es,
            _ => vec![self],
        }
    }

    #[inline]
    pub fn n_args(&self) -> usize {
        match self {
            Expr::Const(_) | Expr::Col(..) | Expr::Plhd(_) | Expr::Subq(..) | Expr::Farg(_) => 0,
            Expr::Aggf(_) => 1,
            Expr::Func(f) => f.args.len(),
            Expr::Pred(p) => match p {
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => es.len(),
                Pred::Not(_) => 1,
                Pred::Func(f) => f.args.len(),
                Pred::InSubquery(..) | Pred::NotInSubquery(..) => 2,
                Pred::Exists(_) | Pred::NotExists(_) => 1,
            },
            Expr::Tuple(es) => es.len(),
        }
    }

    /// Return arguments of current expression.
    /// Many expressions has two arguments so we use SmallVec<[&Expr; 2]>.
    #[inline]
    pub fn args(&self) -> smallvec::IntoIter<[&Expr; 2]> {
        match self {
            Expr::Const(_) | Expr::Col(..) | Expr::Plhd(_) | Expr::Subq(..) | Expr::Farg(_) => {
                smallvec![]
            }
            Expr::Aggf(af) => smallvec![af.arg.as_ref()],
            Expr::Func(f) => SmallVec::from_iter(f.args.iter()),
            Expr::Pred(p) => match p {
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => SmallVec::from_iter(es.iter()),
                Pred::Not(e) => smallvec![e.as_ref()],
                Pred::Func(f) => SmallVec::from_iter(f.args.iter()),
                Pred::InSubquery(lhs, subq) | Pred::NotInSubquery(lhs, subq) => {
                    smallvec![lhs.as_ref(), subq.as_ref()]
                }
                Pred::Exists(subq) | Pred::NotExists(subq) => smallvec![subq.as_ref()],
            },
            Expr::Tuple(es) => SmallVec::from_iter(es.iter()),
        }
        .into_iter()
    }

    /// Returns mutable arguments of current expression.
    #[inline]
    pub fn args_mut(&mut self) -> smallvec::IntoIter<[&mut Expr; 2]> {
        match self {
            Expr::Const(_) | Expr::Col(..) | Expr::Plhd(_) | Expr::Subq(..) | Expr::Farg(_) => {
                smallvec![]
            }
            Expr::Aggf(af) => smallvec![af.arg.as_mut()],
            Expr::Func(f) => SmallVec::from_iter(f.args.iter_mut()),
            Expr::Pred(p) => match p {
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => {
                    SmallVec::from_iter(es.iter_mut())
                }
                Pred::Not(e) => smallvec![e.as_mut()],
                Pred::Func(f) => SmallVec::from_iter(f.args.iter_mut()),
                Pred::InSubquery(lhs, subq) | Pred::NotInSubquery(lhs, subq) => {
                    smallvec![lhs.as_mut(), subq.as_mut()]
                }
                Pred::Exists(subq) | Pred::NotExists(subq) => smallvec![subq.as_mut()],
            },
            Expr::Tuple(es) => SmallVec::from_iter(es.iter_mut()),
        }
        .into_iter()
    }

    pub fn walk<V: ExprVisitor>(&self, visitor: &mut V) -> bool {
        if !visitor.enter(self) {
            return false;
        }
        for c in self.args() {
            if !c.walk(visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }

    pub fn walk_mut<V: ExprMutVisitor>(&mut self, visitor: &mut V) -> bool {
        if !visitor.enter(self) {
            return false;
        }
        for c in self.args_mut() {
            if !c.walk_mut(visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }

    #[inline]
    pub fn collect_non_aggr_cols(&self) -> (Vec<Col>, bool) {
        let mut cols = vec![];
        let has_aggr = self.collect_non_aggr_cols_into(&mut cols);
        (cols, has_aggr)
    }

    /// Collect non-aggr columns and returns true if aggr function exists
    #[inline]
    pub fn collect_non_aggr_cols_into(&self, cols: &mut Vec<Col>) -> bool {
        struct Collect<'a> {
            aggr_lvl: usize,
            has_aggr: bool,
            cols: &'a mut Vec<Col>,
        }
        impl ExprVisitor for Collect<'_> {
            #[inline]
            fn enter(&mut self, e: &Expr) -> bool {
                match e {
                    Expr::Aggf(_) => {
                        self.aggr_lvl += 1;
                        self.has_aggr = true
                    }
                    Expr::Col(col) => {
                        if self.aggr_lvl == 0 {
                            self.cols.push(*col)
                        }
                    }
                    _ => (),
                }
                true
            }

            #[inline]
            fn leave(&mut self, e: &Expr) -> bool {
                if let Expr::Aggf(_) = e {
                    self.aggr_lvl -= 1
                }
                true
            }
        }
        let mut c = Collect {
            aggr_lvl: 0,
            cols,
            has_aggr: false,
        };
        let _ = self.walk(&mut c);
        c.has_aggr
    }

    #[inline]
    pub fn contains_aggr_func(&self) -> bool {
        struct Contains(bool);
        impl ExprVisitor for Contains {
            #[inline]
            fn enter(&mut self, e: &Expr) -> bool {
                if let Expr::Aggf(_) = e {
                    self.0 = true;
                    return false;
                }
                true
            }
        }
        let mut c = Contains(false);
        let _ = self.walk(&mut c);
        c.0
    }

    #[inline]
    pub fn contains_non_aggr_cols(&self) -> bool {
        struct Contains {
            aggr_lvl: usize,
            has_non_aggr_cols: bool,
        }

        impl ExprVisitor for Contains {
            fn enter(&mut self, e: &Expr) -> bool {
                match e {
                    Expr::Aggf(_) => self.aggr_lvl += 1,
                    Expr::Col(_) => {
                        if self.aggr_lvl == 0 {
                            self.has_non_aggr_cols = true;
                            return false;
                        }
                    }
                    _ => (),
                }
                true
            }

            fn leave(&mut self, e: &Expr) -> bool {
                if let Expr::Aggf(_) = e {
                    self.aggr_lvl -= 1
                }
                true
            }
        }

        let mut c = Contains {
            aggr_lvl: 0,
            has_non_aggr_cols: false,
        };
        let _ = self.walk(&mut c);
        c.has_non_aggr_cols
    }

    #[inline]
    pub fn collect_query_ids(&self) -> HashSet<QueryID> {
        let mut c = CollectQryIDs(HashSet::new());
        let _ = self.walk(&mut c);
        c.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Col {
    TableCol(TableID, u32),
    QueryCol(QueryID, u32),
    CorrelatedCol(QueryID, u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Setq {
    All,
    Distinct,
}

impl Setq {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            Setq::All => "all",
            Setq::Distinct => "distinct",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Aggf {
    pub kind: AggKind,
    pub q: Setq,
    pub arg: Box<Expr>,
}

impl Aggf {
    #[inline]
    pub fn n_args(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubqKind {
    Scalar,
    // in and not in
    In,
    // exists and not exists
    Exists,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Plhd {
    Ident(u32),
    Subquery(SubqKind, u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Farg {
    /// indicates there is no argument present in this position of the argument list
    None,
    TimeUnit(TimeUnit),
}

/// QueryID wraps u32 to be the identifier of subqueries in single query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryID(u32);

impl From<u32> for QueryID {
    fn from(src: u32) -> Self {
        debug_assert!(src != !0, "Constructing QueryID from !0 is not allowed");
        QueryID(src)
    }
}

impl Deref for QueryID {
    type Target = u32;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for QueryID {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "q{}", self.0)
    }
}

pub const INVALID_QUERY_ID: QueryID = QueryID(!0);

pub trait ExprVisitor {
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _e: &Expr) -> bool {
        true
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _e: &Expr) -> bool {
        true
    }
}

pub trait ExprMutVisitor {
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _e: &mut Expr) -> bool {
        true
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _e: &mut Expr) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
pub struct CollectQryIDs(HashSet<QueryID>);

impl CollectQryIDs {
    #[inline]
    pub fn res(self) -> HashSet<QueryID> {
        self.0
    }
}

impl ExprVisitor for CollectQryIDs {
    fn leave(&mut self, e: &Expr) -> bool {
        if let Expr::Col(Col::QueryCol(qry_id, _)) = e {
            self.0.insert(*qry_id);
        }
        true
    }
}
