use crate::lgc::{QuerySet, Location};
use crate::lgc::{Op, OpKind, OpVisitor, OpMutVisitor, ProjCol};
use crate::error::{Error, Result};
use xngin_expr::QueryID;
use xngin_datatype::PreciseType;
use std::sync::Arc;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::ControlFlow;

/// Output fix is one step at end of the logical plan optimization.
/// It removes all unnecessary projection nodes and fix output of 
/// each other nodes.
/// 
/// We need to take care of following cases.
/// 
/// 1. invisible column.
/// 
/// Example sql: "SELECT c0 FROM t1 ORDER BY c1".
/// The sort operator at top requires `c1` but projection operator only
/// output `c0`, consistent with output of the whole plan.
/// In such case, we need to add an *invisible* output `c1` to projection
/// to make sure sort has enough data.
/// If all operators support projection, we can remove projection nodes
/// and make the plan compact.
/// 
/// 2. aggregation.
/// 
/// Example sql: "SELECT c0, SUM(c1) + c0 FROM t1 GROUP BY c0".
/// If we separate the calculation into projection and aggregation,
/// we can process `c0` and `SUM(c1)` in aggregation, then perform
/// the addition in projection.
/// If we want to remove projection, we have to let the output stage
/// of aggregation handle the addition.
/// 
/// The basic steps are:
/// 1. Find output columns of top query.
/// 2. Set output columns to root operator.
/// 3. From root to leaf, set output according to each operator's behavior.
#[inline]
pub fn output_fix(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    if let Some(subq) = qry_set.get_mut(&qry_id) {
        let out_cols: Vec<ProjCol> = subq.out_cols().iter().map(|c| c.clone()).collect();
        fix_output(qry_set, qry_id, Arc::new(out_cols))
    } else {
        Err(Error::QueryNotFound(qry_id))
    }
}

#[inline]
fn fix_output(qry_set: &mut QuerySet, qry_id: QueryID, output: Arc<Vec<ProjCol>>) -> Result<()> {
    todo!()
}

/// collect expression of first projection/aggregation/row operator and try to
/// collapse those in common in other nodes. 
struct CollectTopDown {
    out: Vec<ProjCol>,
    // out map stores output expression other than column.
    out_map: HashMap<Expr, usize>,
    out_inited: bool,
}

struct CollectBottomUp {

}

// impl OpVisitor for Collect {
//     type Cont = ();
//     type Break = Error;
//     #[inline]
//     fn leave(&mut self, op: &Op) -> ControlFlow<Error, ()> {
//         match &op.kind {
//             OpKind::Query(_) => (),
//             OpKind::Proj{cols, ..} => {
//                 let cols = cols.as_ref().unwrap();
//                 if !self.out_fixed {
//                     self.out.extend_from_slice(cols);
//                     self.out_fixed = true;
//                 }
//                 for c in cols {
//                     if c.expr.kind.is_col() {

//                     }
//                 }
//             }
//         }
//     }
// }

struct Fix {
    input: Vec<ProjCol>,
    output: Vec<(usize, PreciseType)>,
    // duplicate expressions should be only kept one copy,
    // others can be derived from the first one.
    expr_map: HashMap<Expr, usize>,
}

impl Fix {
    #[inline]
    fn new(out_cols: Vec<ProjCol>) -> Self {
        // let mut expr_map = HashMap::with_capacity(out_cols.len());
        // for c in out_cols.into_iter() {
        //     match expr_map.entry(c.expr) {
        //         Entry::Occupied(occ) => {
        //             // duplicate expression found, we only need child to output the first one
        //             // and make copy of it.
        //             // e.g.   [a, b, b, c] => input=[a, b, c], index=[0, 1, 1, 2]
        //             let output = (*occ.get(), occ.key().ty);

        //         }
        //     }
        // }
        todo!()
    }
}

impl OpMutVisitor for Fix {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, ()> {
        // match &mut op.kind {
        //     OpKind::Proj { cols, input } => {
        //         if let Some(cols) = cols.take() {
        //             // check it
        //         }
        //     }
        //     OpKind::Sort { items, limit, input } => {
                
        //     }
        // }
        todo!()
    }
}
