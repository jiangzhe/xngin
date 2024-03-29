use crate::error::{Error, Result};
use crate::lgc::op::Op;
use xngin_expr::{QueryID, Setq};

#[derive(Debug, Clone)]
pub struct Setop {
    pub kind: SetopKind,
    pub q: Setq,
    /// Sources of Setop are always subqueries.
    pub left: SubqOp,
    pub right: SubqOp,
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

/// SubqOp is subset of Op, which only includes
/// Query and Empty as its variants.
/// Empty is not naturally supported by SQL but can be
/// generated by optimizer.
#[derive(Debug, Clone, Default)]
pub struct SubqOp(Op);

impl SubqOp {
    #[inline]
    pub fn query(qry_id: QueryID) -> Self {
        SubqOp(Op::Query(qry_id))
    }

    #[inline]
    pub fn empty() -> Self {
        SubqOp(Op::Empty)
    }
}

impl From<SubqOp> for Op {
    #[inline]
    fn from(src: SubqOp) -> Self {
        src.0
    }
}

impl TryFrom<Op> for SubqOp {
    type Error = Error;
    #[inline]
    fn try_from(src: Op) -> Result<Self> {
        match src {
            Op::Query(qry_id) => Ok(SubqOp::query(qry_id)),
            Op::Empty => Ok(SubqOp::empty()),
            _ => Err(Error::InvalidOpertorTransformation),
        }
    }
}

impl AsRef<Op> for SubqOp {
    #[inline]
    fn as_ref(&self) -> &Op {
        &self.0
    }
}

impl AsMut<Op> for SubqOp {
    #[inline]
    fn as_mut(&mut self) -> &mut Op {
        &mut self.0
    }
}
