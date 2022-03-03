pub mod graph;

use crate::error::{Error, Result};
use crate::op::Op;
use std::collections::HashSet;
use xngin_expr::{Expr, QueryID};

// aliases of join graph and edge
pub use graph::{Edge as JoinEdge, Graph as JoinGraph};

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

impl Join {
    #[inline]
    pub fn collect_qry_ids(&self, qry_ids: &mut HashSet<QueryID>) {
        match self {
            Join::Cross(jos) => {
                for jo in jos {
                    jo.collect_qry_ids(qry_ids)
                }
            }
            Join::Qualified(QualifiedJoin { left, right, .. }) => {
                left.collect_qry_ids(qry_ids);
                right.collect_qry_ids(qry_ids);
            }
        }
    }

    #[inline]
    pub fn contains_qry_id(&self, qry_id: QueryID) -> bool {
        match self {
            Join::Cross(jos) => jos.iter().any(|jo| jo.contains_qry_id(qry_id)),
            Join::Qualified(QualifiedJoin { left, right, .. }) => {
                left.contains_qry_id(qry_id) || right.contains_qry_id(qry_id)
            }
        }
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
/// Query, Join and Empty as its variants.
/// Empty is not naturally supported by SQL but can be
/// generated by optimizer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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

    #[inline]
    pub fn collect_qry_ids(&self, qry_ids: &mut HashSet<QueryID>) {
        match self.as_ref() {
            Op::Empty => (),
            Op::Query(qry_id) => {
                qry_ids.insert(*qry_id);
            }
            Op::Join(join) => join.collect_qry_ids(qry_ids),
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn contains_qry_id(&self, qry_id: QueryID) -> bool {
        match self.as_ref() {
            Op::Empty => false,
            Op::Query(qid) => *qid == qry_id,
            Op::Join(join) => join.contains_qry_id(qry_id),
            _ => unreachable!(),
        }
    }
}

impl From<JoinOp> for Op {
    #[inline]
    fn from(src: JoinOp) -> Self {
        src.0
    }
}

impl TryFrom<Op> for JoinOp {
    type Error = Error;
    #[inline]
    fn try_from(src: Op) -> Result<Self> {
        match src {
            Op::Query(qry_id) => Ok(JoinOp::query(qry_id)),
            Op::Join(_) => Ok(JoinOp(src)),
            Op::Empty => Ok(JoinOp::empty()),
            _ => Err(Error::InvalidOpertorTransformation),
        }
    }
}

impl AsRef<Op> for JoinOp {
    #[inline]
    fn as_ref(&self) -> &Op {
        &self.0
    }
}

impl AsMut<Op> for JoinOp {
    #[inline]
    fn as_mut(&mut self) -> &mut Op {
        &mut self.0
    }
}
