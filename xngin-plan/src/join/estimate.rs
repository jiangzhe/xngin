use crate::error::Result;
use crate::join::graph::VertexSet;
use crate::join::JoinKind;
use xngin_expr::Expr;

/// Estimates the row count of queries and joins.
/// Query is represented by VertexSet and Join is represented
/// by two VertexSet with join conditions and filter predicates.
pub trait Estimate {
    fn estimated_qry_rows(&mut self, vset: VertexSet) -> Result<f64>;

    fn estimated_join_rows(
        &mut self,
        kind: JoinKind,
        l_vset: VertexSet,
        r_vset: VertexSet,
        e_vset: VertexSet,
        cond: &[Expr],
        filt: &[Expr],
    ) -> Result<f64>;
}
