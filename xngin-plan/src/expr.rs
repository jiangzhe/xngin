use crate::func::{Func, FuncKind};
use crate::id::QueryID;
use crate::pred::{Pred, PredFuncKind};
use smallvec::{smallvec, SmallVec};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use xngin_catalog::{DataType, TableID};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    Const(Const),
    Col(Col),
    AggrFunc(AggrFunc),
    Func(Box<Func>),
    Pred(Box<Pred>),
    Cast(Box<Expr>, DataType),
    Tuple(Vec<Expr>),
    // Subquery that returns single value
    Subq(SubqKind, QueryID),
    // Unknown can represent any intermediate value
    // generated in building phase. It should be
    // resolved as a normal expression later.
    Unknown(Unknown),
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
        Expr::Func(Box::new(Func::new(kind, args)))
    }

    #[inline]
    pub fn pred(p: Pred) -> Self {
        Expr::Pred(Box::new(p))
    }

    #[inline]
    pub fn inner_pred(self) -> Option<Pred> {
        match self {
            Expr::Pred(pred) => Some(*pred),
            _ => None,
        }
    }

    #[inline]
    pub fn pred_func(kind: PredFuncKind, args: Vec<Expr>) -> Self {
        Expr::Pred(Box::new(Pred::func(kind, args)))
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
    pub fn const_str(s: Arc<str>) -> Self {
        Expr::Const(Const::String(s))
    }

    #[inline]
    pub fn const_bytes(bs: Arc<[u8]>) -> Self {
        Expr::Const(Const::Bytes(bs))
    }

    #[inline]
    pub fn count_asterisk() -> Self {
        let af = AggrFunc {
            kind: AggrFuncKind::Count,
            q: SetQuantifier::All,
            expr: Box::new(Expr::const_i64(1)),
        };
        Expr::AggrFunc(af)
    }

    #[inline]
    pub fn count(q: SetQuantifier, expr: Expr) -> Self {
        let af = AggrFunc {
            kind: AggrFuncKind::Count,
            q,
            expr: Box::new(expr),
        };
        Expr::AggrFunc(af)
    }

    #[inline]
    pub fn sum(q: SetQuantifier, expr: Expr) -> Self {
        let af = AggrFunc {
            kind: AggrFuncKind::Sum,
            q,
            expr: Box::new(expr),
        };
        Expr::AggrFunc(af)
    }

    #[inline]
    pub fn avg(q: SetQuantifier, expr: Expr) -> Self {
        let af = AggrFunc {
            kind: AggrFuncKind::Avg,
            q,
            expr: Box::new(expr),
        };
        Expr::AggrFunc(af)
    }

    #[inline]
    pub fn min(q: SetQuantifier, expr: Expr) -> Self {
        let af = AggrFunc {
            kind: AggrFuncKind::Min,
            q,
            expr: Box::new(expr),
        };
        Expr::AggrFunc(af)
    }

    #[inline]
    pub fn max(q: SetQuantifier, expr: Expr) -> Self {
        let af = AggrFunc {
            kind: AggrFuncKind::Max,
            q,
            expr: Box::new(expr),
        };
        Expr::AggrFunc(af)
    }

    #[inline]
    pub fn cast(arg: Expr, ty: DataType) -> Self {
        Expr::Cast(Box::new(arg), ty)
    }

    #[inline]
    pub fn unknown_ident(uid: u32) -> Self {
        Expr::Unknown(Unknown::Ident(uid))
    }

    #[inline]
    pub fn unknown_subquery(uid: u32) -> Self {
        Expr::Unknown(Unknown::Subquery(uid))
    }

    /// Most expression has two children so we use SmallVec<[&Expr; 2]>.
    #[inline]
    pub fn children(&self) -> smallvec::IntoIter<[&Expr; 2]> {
        match self {
            Expr::Const(_) | Expr::Col(..) | Expr::Unknown(_) | Expr::Subq(..) => smallvec![],
            Expr::AggrFunc(af) => smallvec![af.expr.as_ref()],
            Expr::Func(f) => SmallVec::from_iter(f.args.iter()),
            Expr::Pred(p) => match p.as_ref() {
                Pred::True | Pred::False => smallvec![],
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => SmallVec::from_iter(es.iter()),
                Pred::Not(e) => smallvec![e],
                Pred::Func(f) => SmallVec::from_iter(f.args.iter()),
                Pred::InValues(lhs, vals) | Pred::NotInValues(lhs, vals) => {
                    std::iter::once(lhs).chain(vals.iter()).collect()
                }
                Pred::InSubquery(lhs, _) | Pred::NotInSubquery(lhs, _) => smallvec![lhs],
                Pred::Exists(_) | Pred::NotExists(_) => smallvec![],
            },
            Expr::Cast(e, _) => smallvec![e.as_ref()],
            Expr::Tuple(es) => SmallVec::from_iter(es.iter()),
        }
        .into_iter()
    }

    #[inline]
    pub fn children_mut(&mut self) -> smallvec::IntoIter<[&mut Expr; 2]> {
        match self {
            Expr::Const(_) | Expr::Col(..) | Expr::Unknown(_) | Expr::Subq(..) => smallvec![],
            Expr::AggrFunc(af) => smallvec![af.expr.as_mut()],
            Expr::Func(f) => SmallVec::from_iter(f.args.iter_mut()),
            Expr::Pred(p) => match p.as_mut() {
                Pred::True | Pred::False => smallvec![],
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => {
                    SmallVec::from_iter(es.iter_mut())
                }
                Pred::Not(e) => smallvec![e],
                Pred::Func(f) => SmallVec::from_iter(f.args.iter_mut()),
                Pred::InValues(lhs, vals) | Pred::NotInValues(lhs, vals) => {
                    std::iter::once(lhs).chain(vals.iter_mut()).collect()
                }
                Pred::InSubquery(lhs, _) | Pred::NotInSubquery(lhs, _) => smallvec![lhs],
                Pred::Exists(_) | Pred::NotExists(_) => smallvec![],
            },
            Expr::Cast(e, _) => smallvec![e.as_mut()],
            Expr::Tuple(es) => SmallVec::from_iter(es.iter_mut()),
        }
        .into_iter()
    }

    pub fn walk<V: ExprVisitor>(&self, visitor: &mut V) -> bool {
        if !visitor.enter(self) {
            return false;
        }
        for c in self.children() {
            if !c.walk(visitor) {
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
    pub(crate) fn collect_non_aggr_cols_into(&self, cols: &mut Vec<Col>) -> bool {
        let mut collector = CollectNonAggrCols {
            aggr_lvl: 0,
            cols,
            has_aggr: false,
        };
        let _ = self.walk(&mut collector);
        collector.has_aggr
    }

    #[inline]
    pub fn contains_aggr_func(&self) -> bool {
        let mut contains = ContainsAggrFunc::default();
        let _ = self.walk(&mut contains);
        contains.0
    }

    #[inline]
    pub fn contains_non_aggr_cols(&self) -> bool {
        let mut contains = ContainsNonAggrCols::default();
        let _ = self.walk(&mut contains);
        contains.has_non_aggr_cols
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Col {
    TableCol(TableID, u32),
    QueryCol(QueryID, u32),
    CorrelatedCol(QueryID, u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Const {
    I64(i64),
    U64(u64),
    F64(ValidF64),
    Decimal,  // todo
    Datetime, // todo
    String(Arc<str>),
    Bytes(Arc<[u8]>),
}

#[derive(Debug, Clone)]
pub struct ValidF64(f64);

impl ValidF64 {
    #[inline]
    pub fn new(value: f64) -> Option<Self> {
        if value.is_infinite() || value.is_nan() {
            None
        } else {
            Some(ValidF64(value))
        }
    }
}

impl PartialEq for ValidF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

// we must ensure f64 is valid for equality check
impl Eq for ValidF64 {}

impl Hash for ValidF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.to_bits())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetQuantifier {
    All,
    Distinct,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggrFunc {
    pub kind: AggrFuncKind,
    pub q: SetQuantifier,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggrFuncKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubqKind {
    Scalar,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Unknown {
    Ident(u32),
    Subquery(u32),
}

pub trait ExprVisitor {
    /// Returns true if continue
    fn enter(&mut self, e: &Expr) -> bool;

    /// Returns true if continue
    fn leave(&mut self, e: &Expr) -> bool;
}

pub(crate) struct CollectNonAggrCols<'a> {
    aggr_lvl: usize,
    has_aggr: bool,
    cols: &'a mut Vec<Col>,
}

impl ExprVisitor for CollectNonAggrCols<'_> {
    #[inline]
    fn enter(&mut self, e: &Expr) -> bool {
        match e {
            Expr::AggrFunc(_) => {
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
        if let Expr::AggrFunc(_) = e {
            self.aggr_lvl -= 1
        }
        true
    }
}

#[derive(Default)]
pub(crate) struct ContainsAggrFunc(bool);

impl ExprVisitor for ContainsAggrFunc {
    #[inline]
    fn enter(&mut self, e: &Expr) -> bool {
        if let Expr::AggrFunc(_) = e {
            self.0 = true;
            return false;
        }
        true
    }

    #[inline]
    fn leave(&mut self, _e: &Expr) -> bool {
        true
    }
}

#[derive(Default)]
pub(crate) struct ContainsNonAggrCols {
    aggr_lvl: usize,
    has_non_aggr_cols: bool,
}

impl ExprVisitor for ContainsNonAggrCols {
    fn enter(&mut self, e: &Expr) -> bool {
        match e {
            Expr::AggrFunc(_) => self.aggr_lvl += 1,
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
        if let Expr::AggrFunc(_) = e {
            self.aggr_lvl -= 1
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_of_expr() {
        println!("size of Expr {}", std::mem::size_of::<Expr>());
        println!("size of Const {}", std::mem::size_of::<Const>());
        println!("size of Col {}", std::mem::size_of::<Col>());
        println!("size of AggrFunc {}", std::mem::size_of::<AggrFunc>());
        println!("size of Func {}", std::mem::size_of::<Func>());
    }
}
