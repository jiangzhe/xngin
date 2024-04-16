use crate::error::{Error, Result};
use crate::lgc::{Op, OpKind, OpMutVisitor, QuerySet};
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::{GlobalID, QueryID};

#[inline]
pub(super) fn assign_id(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    let mut gid = GlobalID::from(1);
    do_assign_id(qry_set, qry_id, &mut gid)
}

#[inline]
fn do_assign_id(qry_set: &mut QuerySet, qry_id: QueryID, gid: &mut GlobalID) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ai = AssignID { qry_set, gid };
        op.walk_mut(&mut ai).unbranch()
    })?
}

struct AssignID<'a> {
    qry_set: &'a mut QuerySet,
    gid: &'a mut GlobalID,
}

impl OpMutVisitor for AssignID<'_> {
    type Cont = ();
    type Break = Error;

    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error, ()> {
        match &mut op.kind {
            OpKind::Query(qid) => {
                do_assign_id(self.qry_set, *qid, self.gid).branch()?;
                op.id = self.gid.fetch_inc();
                ControlFlow::Continue(())
            }
            _ => {
                op.id = self.gid.fetch_inc();
                ControlFlow::Continue(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{assert_j_plan, print_plan};

    #[test]
    fn test_assign_id() {
        assert_j_plan("select 1 from t1", |sql, mut plan| {
            assign_id(&mut plan.qry_set, plan.root).unwrap();
            print_plan(sql, &plan);
            let ids = collect_id(&mut plan.qry_set, plan.root).unwrap();
            assert_eq!(ids, vec![1, 2, 3]);
        });
        assert_j_plan("select c1 from t1 where c0 = 0", |sql, mut plan| {
            assign_id(&mut plan.qry_set, plan.root).unwrap();
            print_plan(sql, &plan);
            let ids = collect_id(&mut plan.qry_set, plan.root).unwrap();
            assert_eq!(ids, vec![1, 2, 3, 4]);
        });
        assert_j_plan(
            "select 1 from t1, t3 where t1.c1 = t3.c3",
            |sql, mut plan| {
                assign_id(&mut plan.qry_set, plan.root).unwrap();
                print_plan(sql, &plan);
                let ids = collect_id(&mut plan.qry_set, plan.root).unwrap();
                assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7]);
            },
        )
    }

    fn collect_id(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<Vec<u32>> {
        let mut ids = vec![];
        do_collect_id(qry_set, qry_id, &mut ids)?;
        Ok(ids)
    }

    fn do_collect_id(qry_set: &mut QuerySet, qry_id: QueryID, ids: &mut Vec<u32>) -> Result<()> {
        qry_set.transform_op(qry_id, |qry_set, _, op| {
            let mut ai = CollectID { qry_set, ids };
            op.walk_mut(&mut ai).unbranch()
        })?
    }

    struct CollectID<'a> {
        qry_set: &'a mut QuerySet,
        ids: &'a mut Vec<u32>,
    }
    impl OpMutVisitor for CollectID<'_> {
        type Cont = ();
        type Break = Error;

        fn leave(&mut self, op: &mut Op) -> ControlFlow<Self::Break, Self::Cont> {
            match &op.kind {
                OpKind::Query(qid) => {
                    do_collect_id(self.qry_set, *qid, self.ids).branch()?;
                    self.ids.push(op.id.value());
                    ControlFlow::Continue(())
                }
                _ => {
                    self.ids.push(op.id.value());
                    ControlFlow::Continue(())
                }
            }
        }
    }
}
