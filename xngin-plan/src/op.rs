//! This module defines the main algebraic structs used in query plan optimization.
//!
//! The source of the initial plan is an AST directly parsed from SQL, so each plan
//! node should be able to constructed like "raw" SQL format.
//! Identification/normalization is performed at first to convert textual identifiers
//! and expressions to typed/numbered structs for space/processing efficiency, also
//! with schema validated.
//!
//! Each table/column is lookuped from catalog and assigned a unique id.
use smallvec::{smallvec, SmallVec};
use smol_str::SmolStr;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::{Expr, QueryID, Setq};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Proj,
    Filt,
    Aggr,
    Join,
    Sort,
    Limit,
    Apply,
    Row,
    Query,
    Table,
    Setop,
    Empty,
}

/// Op stands for logical operator.
/// This is the general enum containing all nodes of logical plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Projection node.
    Proj(Proj),
    /// Filter node.
    Filt(Filt),
    /// Aggregation node.
    Aggr(Box<Aggr>),
    /// Join node.
    Join(Box<Join>),
    /// Sort node.
    Sort(Sort),
    /// Limit node.
    Limit(Limit),
    /// Apply node.
    Apply(Box<Apply>),
    /// Row represents a single select without source table. e.g. "SELECT 1"
    Row(Vec<(Expr, SmolStr)>),
    /// Query node represents a single row, a concrete table or
    /// a sub-tree containing one or more operators.
    Query(QueryID),
    /// Table node.
    Table(SchemaID, TableID),
    /// Set operations include union, except, intersect.
    Setop(Box<Setop>),
    /// Empty represent a empty data set.
    /// It is used in place for special cases such as impossible predicate,
    /// limit 0 rows, etc.
    Empty,
}

impl Default for Op {
    fn default() -> Self {
        Op::Empty
    }
}

impl Op {
    #[inline]
    pub fn kind(&self) -> OpKind {
        match self {
            Op::Proj(_) => OpKind::Proj,
            Op::Filt(_) => OpKind::Filt,
            Op::Aggr(_) => OpKind::Aggr,
            Op::Join(_) => OpKind::Join,
            Op::Sort(_) => OpKind::Sort,
            Op::Limit(_) => OpKind::Limit,
            Op::Apply(_) => OpKind::Apply,
            Op::Row(_) => OpKind::Row,
            Op::Query(_) => OpKind::Query,
            Op::Table(..) => OpKind::Table,
            Op::Setop(_) => OpKind::Setop,
            Op::Empty => OpKind::Empty,
        }
    }

    #[inline]
    pub fn proj(cols: Vec<(Expr, SmolStr)>, source: Op) -> Self {
        Op::Proj(Proj {
            cols,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn filt(pred: Expr, source: Op) -> Self {
        Op::Filt(Filt {
            pred,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn sort(items: Vec<SortItem>, source: Op) -> Self {
        Op::Sort(Sort {
            items,
            limit: None,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn join(join: Join) -> Self {
        Op::Join(Box::new(join))
    }

    #[inline]
    pub fn aggr(groups: Vec<Expr>, source: Op) -> Self {
        Op::Aggr(Box::new(Aggr {
            groups,
            proj: vec![],
            source,
        }))
    }

    #[inline]
    pub fn limit(start: u64, end: u64, source: Op) -> Self {
        Op::Limit(Limit {
            start,
            end,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn setop(kind: SetopKind, q: Setq, left: Op, right: Op) -> Self {
        Op::Setop(Box::new(Setop {
            kind,
            q,
            left,
            right,
        }))
    }

    #[inline]
    pub fn cross_join(tables: Vec<JoinOp>) -> Self {
        Op::Join(Box::new(Join::Cross(tables)))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self, Op::Empty)
    }

    #[inline]
    pub fn contains_qry(&self, qry_id: QueryID) -> bool {
        struct ContainsQry(QueryID, bool);
        impl OpVisitor for ContainsQry {
            #[inline]
            fn enter(&mut self, op: &Op) -> bool {
                match op {
                    Op::Query(qry_id) if qry_id == &self.0 => {
                        self.1 = true;
                        false
                    }
                    _ => true,
                }
            }
        }

        let mut cq = ContainsQry(qry_id, false);
        let _ = self.walk(&mut cq);
        cq.1
    }

    /// Returns single source of the operator
    #[inline]
    pub fn source_mut(&mut self) -> Option<&mut Op> {
        match self {
            Op::Proj(proj) => Some(&mut proj.source),
            Op::Filt(filt) => Some(&mut filt.source),
            Op::Sort(sort) => Some(&mut sort.source),
            Op::Limit(limit) => Some(&mut limit.source),
            Op::Aggr(aggr) => Some(&mut aggr.source),
            Op::Join(_)
            | Op::Setop(_)
            | Op::Apply(_)
            | Op::Row(_)
            | Op::Table(..)
            | Op::Query(_)
            | Op::Empty => None,
        }
    }

    /// Returns children under current operator until row/table/join/subquery
    #[inline]
    pub fn children(&self) -> SmallVec<[&Op; 2]> {
        match self {
            Op::Proj(proj) => smallvec![proj.source.as_ref()],
            Op::Filt(filt) => smallvec![filt.source.as_ref()],
            Op::Aggr(aggr) => smallvec![&aggr.source],
            Op::Sort(sort) => smallvec![sort.source.as_ref()],
            Op::Limit(limit) => smallvec![limit.source.as_ref()],
            Op::Apply(apply) => smallvec![&apply.left, &apply.right],
            Op::Join(join) => match join.as_ref() {
                Join::Cross(jos) => jos.iter().collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => smallvec![left, right],
            },
            Op::Setop(set) => {
                let Setop { left, right, .. } = set.as_ref();
                smallvec![left, right]
            }
            Op::Query(_) | Op::Row(_) | Op::Table(..) | Op::Empty => smallvec![],
        }
    }

    #[inline]
    pub fn children_mut(&mut self) -> SmallVec<[&mut Op; 2]> {
        match self {
            Op::Proj(proj) => smallvec![proj.source.as_mut()],
            Op::Filt(filt) => smallvec![filt.source.as_mut()],
            Op::Aggr(aggr) => smallvec![&mut aggr.source],
            Op::Sort(sort) => smallvec![sort.source.as_mut()],
            Op::Limit(limit) => smallvec![limit.source.as_mut()],
            Op::Apply(apply) => smallvec![&mut apply.left, &mut apply.right],
            Op::Join(join) => match join.as_mut() {
                Join::Cross(jos) => jos.iter_mut().collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => smallvec![left, right],
            },
            Op::Setop(set) => {
                let Setop { left, right, .. } = set.as_mut();
                smallvec![left, right]
            }
            Op::Query(_) | Op::Row(_) | Op::Table(..) | Op::Empty => smallvec![],
        }
    }

    #[inline]
    pub fn exprs(&self) -> SmallVec<[&Expr; 2]> {
        match self {
            Op::Proj(proj) => proj.cols.iter().map(|(e, _)| e).collect(),
            Op::Filt(filt) => smallvec![&filt.pred],
            Op::Aggr(aggr) => aggr
                .groups
                .iter()
                .chain(aggr.proj.iter().map(|(e, _)| e))
                .collect(),
            Op::Sort(sort) => sort.items.iter().map(|si| &si.expr).collect(),
            Op::Limit(_) | Op::Query(_) | Op::Table(..) | Op::Setop(_) | Op::Empty => {
                smallvec![]
            }
            Op::Apply(apply) => apply.vars.iter().collect(),
            Op::Join(j) => match j.as_ref() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, .. }) => smallvec![cond],
            },
            Op::Row(row) => row.iter().map(|(e, _)| e).collect(),
        }
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> SmallVec<[&mut Expr; 2]> {
        match self {
            Op::Proj(proj) => proj.cols.iter_mut().map(|(e, _)| e).collect(),
            Op::Filt(filt) => smallvec![&mut filt.pred],
            Op::Aggr(aggr) => aggr
                .groups
                .iter_mut()
                .chain(aggr.proj.iter_mut().map(|(e, _)| e))
                .collect(),
            Op::Sort(sort) => sort.items.iter_mut().map(|si| &mut si.expr).collect(),
            Op::Limit(_) | Op::Query(_) | Op::Table(..) | Op::Setop(_) | Op::Empty => {
                smallvec![]
            }
            Op::Apply(apply) => apply.vars.iter_mut().collect(),
            Op::Join(j) => match j.as_mut() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, .. }) => smallvec![cond],
            },
            Op::Row(row) => row.iter_mut().map(|(e, _)| e).collect(),
        }
    }

    pub fn walk<V: OpVisitor>(&self, visitor: &mut V) -> bool {
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

    pub fn walk_mut<V: OpMutVisitor>(&mut self, visitor: &mut V) -> bool {
        if !visitor.enter(self) {
            return false;
        }
        for c in self.children_mut() {
            if !c.walk_mut(visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proj {
    pub cols: Vec<(Expr, SmolStr)>,
    pub source: Box<Op>,
}

/// Filter node.
///
/// Filter result with given predicates.
/// The normal filter won't include projection, but here we
/// add proj to allow combine them into one node.
/// Actually all Scan node will be converted to Filter
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filt {
    pub pred: Expr,
    pub source: Box<Op>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggr {
    pub groups: Vec<Expr>,
    pub proj: Vec<(Expr, SmolStr)>,
    pub source: Op,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrProjKind {
    Group,
    Func,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Full,
    Semi,
    AntiSemi,
    Mark,
    Single,
}

impl JoinKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            JoinKind::Inner => "inner",
            JoinKind::Left => "left",
            JoinKind::Full => "full",
            JoinKind::Semi => "semi",
            JoinKind::AntiSemi => "antisemi",
            JoinKind::Mark => "mark",
            JoinKind::Single => "single",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Join {
    Cross(Vec<JoinOp>),
    /// All natural join are converted to cross join or qualified join.
    Qualified(QualifiedJoin),
}

impl Join {
    #[inline]
    pub fn qualified(kind: JoinKind, left: JoinOp, right: JoinOp, cond: Expr) -> Self {
        Join::Qualified(QualifiedJoin {
            kind,
            left,
            right,
            cond,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaturalJoin {
    pub left: JoinOp,
    pub right: JoinOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedJoin {
    pub kind: JoinKind,
    pub left: JoinOp,
    pub right: JoinOp,
    pub cond: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentJoin {
    pub kind: JoinKind,
    pub left: JoinOp,
    pub right: JoinOp,
    pub cond: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Apply {
    pub kind: ApplyKind,
    /// Free variables of subquery.
    /// If empty, subquery is non-correlated.
    /// Otherwise, correlated.
    pub vars: Vec<Expr>,
    /// Outer query.
    pub left: Op,
    /// inner subquery.
    /// The operator is always Op::Subquery.
    pub right: Op,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyKind {
    /// Right generate single value,
    /// Append it to left for each row.
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setop {
    pub kind: SetopKind,
    pub q: Setq,
    /// Sources of Setop are always subqueries.
    pub left: Op,
    pub right: Op,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetopKind {
    Union,
    Except,
    Intersect,
}

impl SetopKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            SetopKind::Union => "union",
            SetopKind::Except => "except",
            SetopKind::Intersect => "intersect",
        }
    }
}

/// JoinOp is subset of Op, which only includes
/// Subquery and Join as its variants.
pub type JoinOp = Op;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sort {
    pub items: Vec<SortItem>,
    pub limit: Option<u64>,
    pub source: Box<Op>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortItem {
    pub expr: Expr,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    pub start: u64,
    pub end: u64,
    pub source: Box<Op>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    pub expr: Expr,
    pub alias: Option<SmolStr>,
}

pub trait OpVisitor {
    /// Returns true if continue
    fn enter(&mut self, _op: &Op) -> bool {
        true
    }

    /// Returns true if continue
    fn leave(&mut self, _op: &Op) -> bool {
        true
    }
}

pub trait OpMutVisitor {
    /// Returns true if continue
    fn enter(&mut self, _op: &mut Op) -> bool {
        true
    }

    /// Returns true if continue
    fn leave(&mut self, _op: &mut Op) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_logical_nodes() {
        println!("size of Op {}", std::mem::size_of::<Op>());
        println!("size of Proj {}", std::mem::size_of::<Proj>());
        println!("size of Filt {}", std::mem::size_of::<Filt>());
        println!("size of Join {}", std::mem::size_of::<Join>());
        println!("size of JoinKind {}", std::mem::size_of::<JoinKind>());
        println!("size of Aggr {}", std::mem::size_of::<Aggr>());
    }
}
