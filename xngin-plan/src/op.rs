//! This module defines the main algebraic structs used in query plan optimization.
//!
//! The source of the initial plan is an AST directly parsed from SQL, so each plan
//! node should be able to constructed like "raw" SQL format.
//! Identification/normalization is performed at first to convert textual identifiers
//! and expressions to typed/numbered structs for space/processing efficiency, also
//! with schema validated.
//!
//! Each table/column is lookuped from catalog and assigned a unique id.
use crate::error::Error;
use crate::join::{Join, JoinGraph, JoinKind, JoinOp, QualifiedJoin};
use crate::setop::{Setop, SetopKind, SubqOp};
use smallvec::{smallvec, SmallVec};
use smol_str::SmolStr;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::controlflow::ControlFlow;
use xngin_expr::{Expr, QueryID, Setq};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Proj,
    Filt,
    Aggr,
    Join,
    JoinGraph,
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
    /// This is a temporary node only existing in query optimizing phase.
    /// After that, it will be converted back to multiple Join nodes.
    JoinGraph(Box<JoinGraph>),
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
            Op::JoinGraph(_) => OpKind::JoinGraph,
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
    pub fn filt(pred: Vec<Expr>, source: Op) -> Self {
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
    pub fn join_graph(graph: JoinGraph) -> Self {
        Op::JoinGraph(Box::new(graph))
    }

    #[inline]
    pub fn aggr(groups: Vec<Expr>, source: Op) -> Self {
        Op::Aggr(Box::new(Aggr {
            groups,
            proj: vec![],
            source,
            filt: vec![],
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
    pub fn setop(kind: SetopKind, q: Setq, left: SubqOp, right: SubqOp) -> Self {
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
    pub fn qualified_join(
        kind: JoinKind,
        left: JoinOp,
        right: JoinOp,
        cond: Vec<Expr>,
        filt: Vec<Expr>,
    ) -> Self {
        Op::Join(Box::new(Join::Qualified(QualifiedJoin {
            kind,
            left,
            right,
            cond,
            filt,
        })))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self, Op::Empty)
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
            | Op::JoinGraph(_)
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
                Join::Cross(jos) => jos.iter().map(AsRef::as_ref).collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => {
                    smallvec![left.as_ref(), right.as_ref()]
                }
            },
            Op::JoinGraph(graph) => graph.queries.iter().collect(),
            Op::Setop(set) => {
                let Setop { left, right, .. } = set.as_ref();
                smallvec![left.as_ref(), right.as_ref()]
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
                Join::Cross(jos) => jos.iter_mut().map(AsMut::as_mut).collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => {
                    smallvec![left.as_mut(), right.as_mut()]
                }
            },
            Op::JoinGraph(graph) => graph.queries.iter_mut().collect(),
            Op::Setop(set) => {
                let Setop { left, right, .. } = set.as_mut();
                smallvec![left.as_mut(), right.as_mut()]
            }
            Op::Query(_) | Op::Row(_) | Op::Table(..) | Op::Empty => smallvec![],
        }
    }

    #[inline]
    pub fn exprs(&self) -> SmallVec<[&Expr; 2]> {
        match self {
            Op::Proj(proj) => proj.cols.iter().map(|(e, _)| e).collect(),
            Op::Filt(filt) => filt.pred.iter().collect(),
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
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    cond.iter().chain(filt.iter()).collect()
                }
            },
            Op::JoinGraph(graph) => graph
                .edges
                .values()
                .flat_map(|jc| jc.cond.iter().chain(jc.filt.iter()))
                .collect(),
            Op::Row(row) => row.iter().map(|(e, _)| e).collect(),
        }
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> SmallVec<[&mut Expr; 2]> {
        match self {
            Op::Proj(proj) => proj.cols.iter_mut().map(|(e, _)| e).collect(),
            Op::Filt(filt) => filt.pred.iter_mut().collect(),
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
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    cond.iter_mut().chain(filt.iter_mut()).collect()
                }
            },
            Op::JoinGraph(graph) => graph
                .edges
                .values_mut()
                .flat_map(|jc| jc.cond.iter_mut().chain(jc.filt.iter_mut()))
                .collect(),
            Op::Row(row) => row.iter_mut().map(|(e, _)| e).collect(),
        }
    }

    pub fn walk<V: OpVisitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        visitor.enter(self)?;
        for c in self.children() {
            c.walk(visitor)?
        }
        visitor.leave(self)
    }

    pub fn walk_mut<V: OpMutVisitor>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        visitor.enter(self)?;
        for c in self.children_mut() {
            c.walk_mut(visitor)?
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
    pub pred: Vec<Expr>,
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
    // The filter applied after aggregation
    pub filt: Vec<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrProjKind {
    Group,
    Func,
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
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _op: &Op) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _op: &Op) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

/// Helper function to generate a visitor to
/// traverse the operator tree in preorder.
pub fn preorder<F: FnMut(&Op)>(f: F) -> impl OpVisitor {
    struct Preorder<F>(F);
    impl<F: FnMut(&Op)> OpVisitor for Preorder<F> {
        type Break = Error;
        fn enter(&mut self, op: &Op) -> ControlFlow<Error> {
            (self.0)(op);
            ControlFlow::Continue(())
        }
    }
    Preorder(f)
}

pub trait OpMutVisitor {
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _op: &mut Op) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _op: &mut Op) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
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
        println!("size of Aggr {}", std::mem::size_of::<JoinGraph>());
        println!(
            "size of smallvec qids {}",
            std::mem::size_of::<SmallVec<[QueryID; 4]>>()
        );
    }
}
