//! This module defines the main algebraic structs used in query plan optimization.
//!
//! The source of the initial plan is an AST directly parsed from SQL, so each plan
//! node should be able to constructed like "raw" SQL format.
//! Identification/normalization is performed at first to convert textual identifiers
//! and expressions to typed/numbered structs for space/processing efficiency, also
//! with schema validated.
//!
//! Each table/column is lookuped from catalog and assigned a unique id.
use crate::col::ProjCol;
use crate::error::Error;
use crate::join::{Join, JoinGraph, JoinKind, JoinOp, QualifiedJoin};
use crate::setop::{Setop, SetopKind, SubqOp};
use smallvec::{smallvec, SmallVec};
use std::collections::HashSet;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::controlflow::ControlFlow;
use xngin_expr::{Effect, Expr, QueryID, Setq};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Proj,
    Filt,
    Aggr,
    Join,
    JoinGraph,
    Sort,
    Limit,
    Attach,
    Row,
    Query,
    Table,
    Setop,
    Empty,
}

/// Op stands for logical operator.
/// This is the general enum containing all nodes of logical plan.
#[derive(Debug, Clone)]
pub enum Op {
    /// Projection node.
    Proj { cols: Vec<ProjCol>, input: Box<Op> },
    /// Filter node.
    Filt { pred: Vec<Expr>, input: Box<Op> },
    /// Aggregation node.
    Aggr(Box<Aggr>),
    /// Join node.
    Join(Box<Join>),
    /// This is a temporary node only existing in query optimizing phase.
    /// After that, it will be converted back to multiple Join nodes.
    JoinGraph(Box<JoinGraph>),
    /// Sort node.
    Sort {
        items: Vec<SortItem>,
        limit: Option<u64>,
        input: Box<Op>,
    },
    /// Limit node.
    Limit {
        start: u64,
        end: u64,
        input: Box<Op>,
    },
    /// Attach node.
    /// Attach a deferred scalar value into result set.
    /// This node is converted from a non-correlated scalar subquery.
    Attach(Box<Op>, QueryID),
    /// Row represents a single select without source table. e.g. "SELECT 1"
    Row(Vec<ProjCol>),
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
            Op::Proj { .. } => OpKind::Proj,
            Op::Filt { .. } => OpKind::Filt,
            Op::Aggr(_) => OpKind::Aggr,
            Op::Join(_) => OpKind::Join,
            Op::JoinGraph(_) => OpKind::JoinGraph,
            Op::Sort { .. } => OpKind::Sort,
            Op::Limit { .. } => OpKind::Limit,
            Op::Attach(..) => OpKind::Attach,
            Op::Row(_) => OpKind::Row,
            Op::Query(_) => OpKind::Query,
            Op::Table(..) => OpKind::Table,
            Op::Setop(_) => OpKind::Setop,
            Op::Empty => OpKind::Empty,
        }
    }

    #[inline]
    pub fn proj(cols: Vec<ProjCol>, input: Op) -> Self {
        Op::Proj {
            cols,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn filt(pred: Vec<Expr>, input: Op) -> Self {
        Op::Filt {
            pred,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn sort(items: Vec<SortItem>, input: Op) -> Self {
        Op::Sort {
            items,
            limit: None,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn join_graph(graph: JoinGraph) -> Self {
        Op::JoinGraph(Box::new(graph))
    }

    #[inline]
    pub fn aggr(groups: Vec<Expr>, input: Op) -> Self {
        Op::Aggr(Box::new(Aggr {
            groups,
            proj: vec![],
            input,
            filt: vec![],
        }))
    }

    #[inline]
    pub fn limit(start: u64, end: u64, input: Op) -> Self {
        Op::Limit {
            start,
            end,
            input: Box::new(input),
        }
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

    #[inline]
    pub fn out_cols(&self) -> Option<&[ProjCol]> {
        let mut op = self;
        loop {
            match op {
                Op::Aggr(aggr) => return Some(&aggr.proj),
                Op::Proj { cols, .. } => return Some(cols),
                Op::Row(row) => return Some(row),
                Op::Sort { input, .. } => op = input.as_ref(),
                Op::Limit { input, .. } => op = input.as_ref(),
                Op::Filt { input, .. } => op = input.as_ref(),
                Op::Table(..)
                | Op::Query(_)
                | Op::Setop(_)
                | Op::Join(_)
                | Op::JoinGraph(_)
                | Op::Attach(..)
                | Op::Empty => return None,
            }
        }
    }

    #[inline]
    pub fn out_cols_mut(&mut self) -> Option<&mut [ProjCol]> {
        let mut op = self;
        loop {
            match op {
                Op::Aggr(aggr) => return Some(&mut aggr.proj),
                Op::Proj { cols, .. } => return Some(cols),
                Op::Row(row) => return Some(row.as_mut()),
                Op::Sort { input, .. } => op = input.as_mut(),
                Op::Limit { input, .. } => op = input.as_mut(),
                Op::Filt { input, .. } => op = input.as_mut(),
                Op::Table(..)
                | Op::Query(_)
                | Op::Setop(_)
                | Op::Join(_)
                | Op::JoinGraph(_)
                | Op::Attach(..)
                | Op::Empty => return None,
            }
        }
    }

    /// Returns single source of the operator
    #[inline]
    pub fn input_mut(&mut self) -> Option<&mut Op> {
        match self {
            Op::Proj { input, .. } => Some(input),
            Op::Filt { input, .. } => Some(input),
            Op::Sort { input, .. } => Some(input),
            Op::Limit { input, .. } => Some(input),
            Op::Aggr(aggr) => Some(&mut aggr.input),
            Op::Join(_)
            | Op::JoinGraph(_)
            | Op::Setop(_)
            | Op::Attach(..)
            | Op::Row(_)
            | Op::Table(..)
            | Op::Query(_)
            | Op::Empty => None,
        }
    }

    /// Returns children under current operator until row/table/join/subquery
    #[inline]
    pub fn inputs(&self) -> SmallVec<[&Op; 2]> {
        match self {
            Op::Proj { input, .. } => smallvec![input.as_ref()],
            Op::Filt { input, .. } => smallvec![input.as_ref()],
            Op::Aggr(aggr) => smallvec![&aggr.input],
            Op::Sort { input, .. } => smallvec![input.as_ref()],
            Op::Limit { input, .. } => smallvec![input.as_ref()],
            Op::Attach(c, _) => smallvec![c.as_ref()],
            Op::Join(join) => match join.as_ref() {
                Join::Cross(jos) => jos.iter().map(AsRef::as_ref).collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => {
                    smallvec![left.as_ref(), right.as_ref()]
                }
            },
            Op::JoinGraph(graph) => graph.children(),
            Op::Setop(set) => {
                let Setop { left, right, .. } = set.as_ref();
                smallvec![left.as_ref(), right.as_ref()]
            }
            Op::Query(_) | Op::Row(_) | Op::Table(..) | Op::Empty => smallvec![],
        }
    }

    #[inline]
    pub fn inputs_mut(&mut self) -> SmallVec<[&mut Op; 2]> {
        match self {
            Op::Proj { input, .. } => smallvec![input.as_mut()],
            Op::Filt { input, .. } => smallvec![input.as_mut()],
            Op::Aggr(aggr) => smallvec![&mut aggr.input],
            Op::Sort { input, .. } => smallvec![input.as_mut()],
            Op::Limit { input, .. } => smallvec![input.as_mut()],
            Op::Attach(c, _) => smallvec![c.as_mut()],
            Op::Join(join) => match join.as_mut() {
                Join::Cross(jos) => jos.iter_mut().map(AsMut::as_mut).collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => {
                    smallvec![left.as_mut(), right.as_mut()]
                }
            },
            Op::JoinGraph(graph) => graph.children_mut(),
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
            Op::Proj { cols, .. } => cols.iter().map(|c| &c.expr).collect(),
            Op::Filt { pred, .. } => pred.iter().collect(),
            Op::Aggr(aggr) => aggr
                .groups
                .iter()
                .chain(aggr.proj.iter().map(|c| &c.expr))
                .collect(),
            Op::Sort { items, .. } => items.iter().map(|si| &si.expr).collect(),
            Op::Limit { .. }
            | Op::Query(_)
            | Op::Table(..)
            | Op::Setop(_)
            | Op::Empty
            | Op::Attach(..) => {
                smallvec![]
            }
            Op::Join(j) => match j.as_ref() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    cond.iter().chain(filt.iter()).collect()
                }
            },
            Op::JoinGraph(graph) => graph.exprs().into_iter().collect(),
            Op::Row(row) => row.iter().map(|c| &c.expr).collect(),
        }
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> SmallVec<[&mut Expr; 2]> {
        match self {
            Op::Proj { cols, .. } => cols.iter_mut().map(|c| &mut c.expr).collect(),
            Op::Filt { pred, .. } => pred.iter_mut().collect(),
            Op::Aggr(aggr) => aggr
                .groups
                .iter_mut()
                .chain(aggr.proj.iter_mut().map(|c| &mut c.expr))
                .collect(),
            Op::Sort { items, .. } => items.iter_mut().map(|si| &mut si.expr).collect(),
            Op::Limit { .. }
            | Op::Query(_)
            | Op::Table(..)
            | Op::Setop(_)
            | Op::Empty
            | Op::Attach(..) => {
                smallvec![]
            }
            Op::Join(j) => match j.as_mut() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    cond.iter_mut().chain(filt.iter_mut()).collect()
                }
            },
            Op::JoinGraph(graph) => graph.exprs_mut().into_iter().collect(),
            Op::Row(row) => row.iter_mut().map(|c| &mut c.expr).collect(),
        }
    }

    #[inline]
    pub fn collect_qry_ids(&self, qry_ids: &mut HashSet<QueryID>) {
        struct Collect<'a>(&'a mut HashSet<QueryID>);
        impl OpVisitor for Collect<'_> {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, op: &Op) -> ControlFlow<()> {
                if let Op::Query(qry_id) = op {
                    self.0.insert(*qry_id);
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = Collect(qry_ids);
        let _ = self.walk(&mut c);
    }

    pub fn walk<V: OpVisitor>(&self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.inputs() {
            eff.merge(c.walk(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }

    pub fn walk_mut<V: OpMutVisitor>(&mut self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.inputs_mut() {
            eff.merge(c.walk_mut(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone)]
pub struct Aggr {
    pub groups: Vec<Expr>,
    pub proj: Vec<ProjCol>,
    pub input: Op,
    // The filter applied after aggregation
    pub filt: Vec<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrProjKind {
    Group,
    Func,
}

#[derive(Debug, Clone)]
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
pub struct SortItem {
    pub expr: Expr,
    pub desc: bool,
}

pub trait OpVisitor {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _op: &Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _op: &Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

/// Helper function to generate a visitor to
/// traverse the operator tree in preorder.
pub fn preorder<F: FnMut(&Op)>(f: F) -> impl OpVisitor {
    struct Preorder<F>(F);
    impl<F: FnMut(&Op)> OpVisitor for Preorder<F> {
        type Cont = ();
        type Break = Error;
        fn enter(&mut self, op: &Op) -> ControlFlow<Error> {
            (self.0)(op);
            ControlFlow::Continue(())
        }
    }
    Preorder(f)
}

pub trait OpMutVisitor {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _op: &mut Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _op: &mut Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_logical_nodes() {
        use std::mem::size_of;
        println!("size of Op {}", size_of::<Op>());
        println!("size of Join {}", size_of::<Join>());
        println!("size of JoinKind {}", size_of::<JoinKind>());
        println!("size of Aggr {}", size_of::<Aggr>());
        println!("size of JoinGraph {}", size_of::<JoinGraph>());
        println!(
            "size of SmallVec<[QueryID;4]> is {}",
            size_of::<SmallVec<[QueryID; 4]>>()
        );
        println!(
            "size of SmallVec<[QueryID;5]> is {}",
            size_of::<SmallVec<[QueryID; 5]>>()
        );
    }
}
