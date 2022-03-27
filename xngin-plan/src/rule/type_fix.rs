use crate::error::{Error, Result};
use crate::join::{Join, QualifiedJoin};
use crate::op::{Aggr, Op, OpMutVisitor};
use crate::query::QuerySet;
use std::collections::HashMap;
use xngin_datatype::PreciseType;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::infer::{fix_bools, fix_rec};
use xngin_expr::{Expr, QueryID};

/// Fix types of all expressions.
/// In initialization of logical plan, all type info, except table columns, are left as empty.
/// It's convenient to exclude types in other optimization rules.
/// But at the end, we need to fix types of all expressions to enable efficient evaluation.
#[inline]
pub fn type_fix(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    let mut types = HashMap::new();
    fix_type(qry_set, qry_id, &mut types)
}

#[inline]
fn fix_type(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    types: &mut HashMap<(QueryID, u32), PreciseType>,
) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ft = FixType { qry_set, types };
        op.walk_mut(&mut ft).unbranch()
    })?
}

struct FixType<'a> {
    qry_set: &'a mut QuerySet,
    types: &'a mut HashMap<(QueryID, u32), PreciseType>,
}

impl FixType<'_> {
    #[inline]
    fn fix(&self, e: &mut Expr) -> Result<()> {
        fix_rec(e, |qid, idx| self.types.get(&(qid, idx)).cloned()).map_err(Into::into)
    }
}

impl OpMutVisitor for FixType<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            Op::Query(qry_id) => {
                let qry_id = *qry_id;
                // first, recursively fix types in child query
                fix_type(self.qry_set, qry_id, self.types).branch()?;
                // then populate type map with out columns
                if let Some(subq) = self.qry_set.get(&qry_id) {
                    for (i, (e, _)) in subq.out_cols().iter().enumerate() {
                        if e.ty.is_unknown() {
                            return ControlFlow::Break(Error::TypeInferFailed);
                        } else {
                            self.types.insert((qry_id, i as u32), e.ty);
                        }
                    }
                }
                ControlFlow::Continue(())
            }
            Op::Filt { pred, .. } => {
                for e in pred.iter_mut() {
                    self.fix(e).branch()?
                }
                fix_bools(pred.as_mut());
                ControlFlow::Continue(())
            }
            Op::Join(join) => {
                match join.as_mut() {
                    Join::Cross(_) => (),
                    Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                        for e in cond.iter_mut() {
                            self.fix(e).branch()?
                        }
                        fix_bools(cond.as_mut());
                        for e in filt.iter_mut() {
                            self.fix(e).branch()?
                        }
                        fix_bools(filt.as_mut());
                    }
                }
                ControlFlow::Continue(())
            }
            Op::Aggr(aggr) => {
                let Aggr {
                    groups, proj, filt, ..
                } = aggr.as_mut();
                for e in groups.iter_mut() {
                    self.fix(e).branch()?
                }
                for (e, _) in proj.iter_mut() {
                    self.fix(e).branch()?
                }
                for e in filt.iter_mut() {
                    self.fix(e).branch()?
                }
                fix_bools(filt.as_mut());
                ControlFlow::Continue(())
            }
            _ => {
                for e in op.exprs_mut() {
                    self.fix(e).branch()?
                }
                ControlFlow::Continue(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{build_plan, get_filt_expr, print_plan};
    use crate::lgc::LgcPlan;
    use xngin_catalog::mem_impl::{ColumnSpec, MemCatalog, MemCatalogBuilder};
    use xngin_catalog::{ColumnAttr, QueryCatalog};

    macro_rules! vec2 {
        ($e:expr) => {
            vec![$e, $e]
        };
    }

    #[test]
    fn test_type_fix_add_sub() {
        use xngin_datatype::PreciseType as PT;
        let cat = ty_catalog();
        for (sql, tys) in vec![
            // constants
            (
                "select 1, 1.0, 1e0 from ty1",
                vec![PT::i64(), PT::decimal(2, 1), PT::f64()],
            ),
            // float
            (
                "select c_f64 + c_f64, c_f64 + c_i32, c_i32 + c_f64 from ty1",
                vec![PT::f64(), PT::f64(), PT::f64()],
            ),
            (
                "select c_f32 + c_f64, c_f64 + c_f32 from ty1",
                vec![PT::f64(), PT::f64()],
            ),
            // decimal
            (
                "select c_dec + c_dec, c_dec + c_i32, c_i32 + c_dec from ty1",
                vec![PT::decimal(18, 2), PT::decimal(18, 2), PT::decimal(18, 2)],
            ),
            (
                "select d50 + d50, d50 + d40, d50 + d60 from dec1",
                vec![PT::decimal(5, 0), PT::decimal(5, 0), PT::decimal(6, 0)],
            ),
            (
                "select d51 + d40, d51 + d62 from dec1",
                vec![PT::decimal(5, 1), PT::decimal(6, 2)],
            ),
            (
                "select d61 + d40, d40 + d61 from dec1",
                vec![PT::decimal(6, 1), PT::decimal(6, 1)],
            ),
            (
                "select d52 + d61, d61 + d52 from dec1",
                vec![PT::decimal(7, 2), PT::decimal(7, 2)],
            ),
            // bool
            (
                "select c_bool + c_bool, c_bool + c_i32, c_i32 + c_bool from ty1",
                vec![PT::u64(), PT::u64(), PT::u64()],
            ),
            // int
            (
                "select c_i32 + c_i32, c_i32 + c_u32, c_u32 + c_i32, c_u32 + c_u32 from ty1",
                vec![PT::i32(), PT::u32(), PT::u32(), PT::u32()],
            ),
            (
                "select c_i32 + c_i64, c_i64 + c_i32, c_u32 + c_i64, c_u64 + c_i32 from ty1",
                vec![PT::i64(), PT::i64(), PT::i64(), PT::u64()],
            ),
            // datetime
            (
                "select c_dm + interval '1' day, interval '1' day + c_dm, 'C' + c_dm from ty1",
                vec![PT::datetime(0), PT::datetime(0), PT::f64()],
            ),
            (
                "select dm0 + c_i32, c_i32 + dm0, dm3 + c_i32, c_i32 + dm3 from ty1, dm1",
                vec![PT::i64(), PT::i64(), PT::decimal(18, 3), PT::decimal(18, 3)]
            ),
            (
                "select dm0 + c_bool, dm3 + c_bool from ty1, dm1",
                vec![PT::u64(), PT::decimal(18, 3)],
            ),
            (
                "select dm0 + dm0, dm3 + dm3, dm3 + c_dt from ty1, dm1",
                vec![PT::i64(), PT::decimal(18, 3), PT::decimal(18, 3)],
            ),
            // date
            (
                "select c_dt + interval '1' day, interval '1' day + c_dt, c_dt + c_ascii from ty1",
                vec![PT::date(), PT::date(), PT::f64()],
            ),
            (
                "select c_dt + c_i32, c_i32 + c_dt, c_dt + c_bool, c_dt + c_dt from ty1",
                vec![PT::i64(), PT::i64(), PT::u64(), PT::i64()],
            ),
            // time
            (
                "select c_tm + interval '1' hour, c_tm + c_i32, c_tm + c_ascii, c_tm + c_bool from ty1",
                vec![PT::time(0), PT::i64(), PT::f64(), PT::u64()],
            ),
            (
                "select tm0 + tm0, tm3 + tm3, tm3 + c_dt from ty1, dm1",
                vec![PT::i64(), PT::decimal(18, 3), PT::decimal(18, 3)],
            ),
            // string
            (
                "select a1 + a1, b1 + b1, u1 + u1, a1 + b1, a1 + u1, b1 + u1 from str1",
                vec![PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64()],
            ),
            (
                "select va1 + va1, vb1 + vb1, vu1 + vu1, va1 + vb1, va1 + vu1, vb1 + vu1 from str1",
                vec![PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64()],
            ),
        ] {
            assert_ty_plan1(&cat, sql, |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                assert_eq!(tys, extract_proj_types(&p));
            })
        }
    }

    #[test]
    fn test_type_fix_cmp() {
        use xngin_datatype::PreciseType as PT;
        let cat = ty_catalog();
        for (sql, tys) in vec![
            // float
            (
                "select c_f64 = c_f64, c_f64 > c_i32, c_i32 >= c_f64, c_f64 <> 'abc' from ty1",
                vec![vec2![PT::f64()], vec2![PT::f64()], vec2![PT::f64()], vec2![PT::f64()]],
            ),
            (
                "select c_f64 = c_f32, c_f32 = c_f64 from ty1",
                vec![vec2![PT::f64()], vec2![PT::f64()]],
            ),
            // decimal
            (
                "select c_dec < c_dec, c_dec <= c_i32, c_i32 <=> c_dec from ty1",
                vec![vec2![PT::decimal(18, 2)], vec2![PT::decimal(18, 2)], vec2![PT::decimal(18, 2)]],
            ),
            (
                "select d50 > d40, d50 < d60 from dec1",
                vec![vec2![PT::decimal(5, 0)], vec2![PT::decimal(6, 0)]],
            ),
            (
                "select d51 = d40, d40 = d51 from dec1",
                vec![vec2![PT::decimal(5, 1)], vec2![PT::decimal(5, 1)]],
            ),
            (
                "select d61 = d40, d40 = d61 from dec1",
                vec![vec2![PT::decimal(6, 1)], vec2![PT::decimal(6, 1)]],
            ),
            (
                "select d62 = d50, d50 = d62 from dec1",
                vec![vec2![PT::decimal(7, 2)], vec2![PT::decimal(7, 2)]],
            ),
            // bool
            (
                "select c_bool = c_bool, c_bool = c_i32, c_i32 = c_bool from ty1",
                vec![vec![PT::bool(), PT::bool()], vec![PT::bool(), PT::bool()], vec![PT::bool(), PT::bool()]],
            ),
            // int
            (
                "select c_i32 = c_i32, c_i32 = c_u32, c_u64 = c_i64 from ty1",
                vec![vec2![PT::i32()], vec![PT::i32(), PT::u32()], vec![PT::u64(), PT::i64()]],
            ),
            (
                "select c_i32 = c_i64, c_u64 = c_i32 from ty1",
                vec![vec2![PT::i64()], vec![PT::u64(), PT::i64()]],
            ),
            // datetime
            (
                "select c_dm = c_dm, c_dm = c_i32, c_u32 = c_dm, c_dm = '1' from ty1",
                vec![vec![PT::datetime(0), PT::datetime(0)], vec![PT::i64(), PT::i64()], vec![PT::u64(), PT::u64()], vec![PT::datetime(6), PT::datetime(6)]],
            ),
            (
                "select dm0 < dm3, dm3 < dm0 from dm1",
                vec![vec2![PT::datetime(3)], vec2![PT::datetime(3)]],
            ),
            // date
            (
                "select c_dt = c_dt, c_dt = c_u32, c_i64 = c_dt, c_dt = c_ascii, c_dt = c_bool from ty1",
                vec![vec![PT::date(), PT::date()], vec![PT::u64(), PT::u64()], vec![PT::i64(), PT::i64()], vec![PT::datetime(6), PT::datetime(6)], vec![PT::u64(), PT::u64()]],
            ),
            // time
            (
                "select c_tm = c_tm, c_tm = c_i32, c_u32 = c_tm, c_bool = c_tm from ty1",
                vec![vec![PT::time(0), PT::time(0)], vec![PT::i64(), PT::i64()], vec![PT::u64(), PT::u64()], vec![PT::u64(), PT::u64()]],
            ),
            (
                "select tm0 <= tm3, tm3 >= tm0 from dm1",
                vec![vec2![PT::time(3)], vec2![PT::time(3)]],
            ),
            // string
            (
                "select c_ascii = c_ascii, c_ascii = c_utf8, c_ascii = c_bin from ty1",
                vec![vec![PT::ascii(1), PT::ascii(1)], vec![PT::var_utf8(10), PT::var_utf8(10)], vec![PT::var_bytes(1024), PT::var_bytes(1024)]],
            ),
            (
                "select a1 = a1, a1 = a10, a10 = a1 from str1",
                vec![vec2![PT::ascii(1)], vec2![PT::ascii(10)], vec2![PT::ascii(10)]],
            ),
            (
                "select u1 = u1, u1 = u10, u10 = u1 from str1",
                vec![vec2![PT::utf8(1)], vec2![PT::utf8(10)], vec2![PT::utf8(10)]],
            ),
            (
                "select b1 = b1, b1 = b10, b10 = b1 from str1",
                vec![vec2![PT::bytes(1)], vec2![PT::bytes(10)], vec2![PT::bytes(10)]],
            ),
            (
                "select a10 = b10, a10 = u10, b10 = u10 from str1",
                vec![vec2![PT::bytes(10)], vec2![PT::utf8(10)], vec2![PT::bytes(10)]],
            ),
            (
                "select va1 = va10, va10 = va1 from str1",
                vec![vec![PT::var_ascii(1), PT::var_ascii(10)], vec![PT::var_ascii(10), PT::var_ascii(1)]],
            ),
            (
                "select vu1 = vu10, vu10 = vu1 from str1",
                vec![vec![PT::var_utf8(1), PT::var_utf8(10)], vec![PT::var_utf8(10), PT::var_utf8(1)]],
            ),
            (
                "select vb1 = vb10, vb10 = vb1 from str1",
                vec![vec![PT::var_bytes(1), PT::var_bytes(10)], vec![PT::var_bytes(10), PT::var_bytes(1)]],
            ),
            (
                "select va10 = vb10, va10 = vu10, vb10 = vu10 from str1",
                vec![vec2![PT::var_bytes(10)], vec2![PT::var_utf8(10)], vec2![PT::var_bytes(10)]],
            ),
        ] {
            assert_ty_plan1(&cat, sql, |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                assert_eq!(tys, extract_child_types(&p));
            })
        }
    }

    #[test]
    fn test_type_fix_other_pred() {
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select c_i32 and c_u32, c_f64 or c_ascii, c_dec xor c_ascii, not c_utf8 from ty1",
            |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                for ty in extract_child_types(&p) {
                    assert!(ty.iter().all(PreciseType::is_bool))
                }
            },
        )
    }

    #[test]
    fn test_type_fix_filt() {
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select 1 from ty1 where c_i32 and c_dec",
            |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                for e in get_filt_expr(&p) {
                    assert!(e.ty.is_bool())
                }
            },
        )
    }

    #[test]
    fn test_type_fix_join() {
        use crate::op::{preorder, Op};
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select 1 from ty1 join ty1 as ty2 on ty1.c_i32 = ty2.c_i64 and ty1.c_u64 and ty2.c_dec", |sql, mut p| {
            type_fix(&mut p.qry_set, p.root).unwrap();
            print_plan(sql, &p);
            let subq = p.root_query().unwrap();
            subq.root.walk(&mut preorder(|op| match op {
                j @ Op::Join(_) => {
                    for e in j.exprs() {
                        assert!(e.ty.is_bool());
                    }
                }
                _ => (),
            }));
        })
    }

    #[test]
    fn test_type_fix_aggr() {
        use crate::op::{preorder, Op};
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select c_i32 from ty1 group by c_i32 having c_i32",
            |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                let subq = p.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match op {
                    Op::Aggr(aggr) => {
                        for e in &aggr.filt {
                            assert!(e.ty.is_bool());
                        }
                    }
                    _ => (),
                }));
            },
        )
    }

    #[inline]
    fn ty_catalog() -> MemCatalog {
        let mut builder = MemCatalogBuilder::default();
        builder.add_schema("ty").unwrap();
        builder
            .add_table(
                "ty",
                "ty1",
                &[
                    ColumnSpec::new("c_i32", PreciseType::i32(), ColumnAttr::empty()),
                    ColumnSpec::new("c_u32", PreciseType::u32(), ColumnAttr::empty()),
                    ColumnSpec::new("c_i64", PreciseType::i64(), ColumnAttr::empty()),
                    ColumnSpec::new("c_u64", PreciseType::u64(), ColumnAttr::empty()),
                    ColumnSpec::new("c_f32", PreciseType::f32(), ColumnAttr::empty()),
                    ColumnSpec::new("c_f64", PreciseType::f64(), ColumnAttr::empty()),
                    ColumnSpec::new("c_dec", PreciseType::decimal(18, 2), ColumnAttr::empty()),
                    ColumnSpec::new("c_bool", PreciseType::bool(), ColumnAttr::empty()),
                    ColumnSpec::new("c_dm", PreciseType::datetime(0), ColumnAttr::empty()),
                    ColumnSpec::new("c_tm", PreciseType::time(0), ColumnAttr::empty()),
                    ColumnSpec::new("c_dt", PreciseType::date(), ColumnAttr::empty()),
                    ColumnSpec::new("c_ascii", PreciseType::ascii(1), ColumnAttr::empty()),
                    ColumnSpec::new("c_utf8", PreciseType::var_utf8(10), ColumnAttr::empty()),
                    ColumnSpec::new("c_bin", PreciseType::var_bytes(1024), ColumnAttr::empty()),
                ],
            )
            .unwrap();
        builder
            .add_table(
                "ty",
                "dec1",
                &[
                    ColumnSpec::new("d40", PreciseType::decimal(4, 0), ColumnAttr::empty()),
                    ColumnSpec::new("d50", PreciseType::decimal(5, 0), ColumnAttr::empty()),
                    ColumnSpec::new("d60", PreciseType::decimal(6, 0), ColumnAttr::empty()),
                    ColumnSpec::new("d41", PreciseType::decimal(4, 1), ColumnAttr::empty()),
                    ColumnSpec::new("d51", PreciseType::decimal(5, 1), ColumnAttr::empty()),
                    ColumnSpec::new("d61", PreciseType::decimal(6, 1), ColumnAttr::empty()),
                    ColumnSpec::new("d42", PreciseType::decimal(4, 2), ColumnAttr::empty()),
                    ColumnSpec::new("d52", PreciseType::decimal(5, 2), ColumnAttr::empty()),
                    ColumnSpec::new("d62", PreciseType::decimal(6, 2), ColumnAttr::empty()),
                ],
            )
            .unwrap();
        builder
            .add_table(
                "ty",
                "dm1",
                &[
                    ColumnSpec::new("dm0", PreciseType::datetime(0), ColumnAttr::empty()),
                    ColumnSpec::new("dm3", PreciseType::datetime(3), ColumnAttr::empty()),
                    ColumnSpec::new("dm6", PreciseType::datetime(6), ColumnAttr::empty()),
                    ColumnSpec::new("tm0", PreciseType::time(0), ColumnAttr::empty()),
                    ColumnSpec::new("tm3", PreciseType::time(3), ColumnAttr::empty()),
                    ColumnSpec::new("tm6", PreciseType::time(6), ColumnAttr::empty()),
                ],
            )
            .unwrap();
        builder
            .add_table(
                "ty",
                "str1",
                &[
                    ColumnSpec::new("a1", PreciseType::ascii(1), ColumnAttr::empty()),
                    ColumnSpec::new("a10", PreciseType::ascii(10), ColumnAttr::empty()),
                    ColumnSpec::new("va1", PreciseType::var_ascii(1), ColumnAttr::empty()),
                    ColumnSpec::new("va10", PreciseType::var_ascii(10), ColumnAttr::empty()),
                    ColumnSpec::new("u1", PreciseType::utf8(1), ColumnAttr::empty()),
                    ColumnSpec::new("u10", PreciseType::utf8(10), ColumnAttr::empty()),
                    ColumnSpec::new("vu1", PreciseType::var_utf8(1), ColumnAttr::empty()),
                    ColumnSpec::new("vu10", PreciseType::var_utf8(10), ColumnAttr::empty()),
                    ColumnSpec::new("b1", PreciseType::bytes(1), ColumnAttr::empty()),
                    ColumnSpec::new("b10", PreciseType::bytes(10), ColumnAttr::empty()),
                    ColumnSpec::new("vb1", PreciseType::var_bytes(1), ColumnAttr::empty()),
                    ColumnSpec::new("vb10", PreciseType::var_bytes(10), ColumnAttr::empty()),
                ],
            )
            .unwrap();
        builder.build()
    }

    fn assert_ty_plan1<C: QueryCatalog, F: FnOnce(&str, LgcPlan)>(cat: &C, sql: &str, f: F) {
        let plan = build_plan(cat, "ty", sql);
        f(sql, plan)
    }

    #[inline]
    fn extract_proj_types(p: &LgcPlan) -> Vec<PreciseType> {
        if let Some(subq) = p.root_query() {
            subq.out_cols().iter().map(|(e, _)| e.ty).collect()
        } else {
            vec![]
        }
    }

    #[inline]
    fn extract_child_types(p: &LgcPlan) -> Vec<Vec<PreciseType>> {
        if let Some(subq) = p.root_query() {
            subq.out_cols()
                .iter()
                .map(|(e, _)| e.args().into_iter().map(|arg| arg.ty).collect())
                .collect()
        } else {
            vec![]
        }
    }
}
