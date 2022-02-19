pub mod edge;
pub mod graph;
pub mod vertex;

use crate::op::Op;
use std::ops::{Deref, DerefMut};
use xngin_expr::{Expr, QueryID};

// aliases of join graph and edge
pub use edge::Edge as JoinEdge;
pub use graph::Graph as JoinGraph;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
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
    // pub cond: Expr,
    // Join condition
    pub cond: Vec<Expr>,
    // Additional filter applied after join.
    // Initialized as empty but can be filled in optimization.
    pub filt: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentJoin {
    pub kind: JoinKind,
    pub left: JoinOp,
    pub right: JoinOp,
    pub cond: Expr,
}

/// JoinOp is subset of Op, which only includes
/// Subquery, Join and Empty as its variants.
/// Empty is not naturally supported by SQL but can be
/// generated by optimizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOp(pub(crate) Op);

impl JoinOp {
    #[inline]
    pub fn query(qry_id: QueryID) -> Self {
        JoinOp(Op::Query(qry_id))
    }

    #[inline]
    pub fn cross(tables: Vec<JoinOp>) -> Self {
        JoinOp(Op::cross_join(tables))
    }

    #[inline]
    pub fn empty() -> Self {
        JoinOp(Op::Empty)
    }

    #[inline]
    pub fn qualified(
        kind: JoinKind,
        left: JoinOp,
        right: JoinOp,
        cond: Vec<Expr>,
        filt: Vec<Expr>,
    ) -> Self {
        JoinOp(Op::qualified_join(kind, left, right, cond, filt))
    }
}

impl AsRef<Op> for JoinOp {
    #[inline]
    fn as_ref(&self) -> &Op {
        &self.0
    }
}

impl Deref for JoinOp {
    type Target = Op;
    #[inline]
    fn deref(&self) -> &Op {
        &self.0
    }
}

impl AsMut<Op> for JoinOp {
    #[inline]
    fn as_mut(&mut self) -> &mut Op {
        &mut self.0
    }
}

impl DerefMut for JoinOp {
    #[inline]
    fn deref_mut(&mut self) -> &mut Op {
        &mut self.0
    }
}

impl From<JoinOp> for Op {
    fn from(src: JoinOp) -> Self {
        src.0
    }
}
