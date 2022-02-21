use super::*;
use crate::op::OpKind;
use crate::op::OpVisitor;
use std::sync::Arc;
use xngin_catalog::mem_impl::{ColumnSpec, MemCatalogBuilder};
use xngin_catalog::ColumnAttr;
use xngin_datatype::DataType;
use xngin_frontend::parser::dialect::MySQL;
use xngin_frontend::parser::parse_query;

macro_rules! plan_shape {
    ( $($ok:ident),* ) => {
        vec![
            $(
                OpKind::$ok
            ),*
        ]
    }
}

#[test]
fn test_plan_build_select_row() {
    let cat = empty_catalog();
    let shape = plan_shape![Row];
    for sql in vec![
        "select 1",
        "select 1, 2, 3",
        "select 1 as a",
        "select 1 as a, 2 as b",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "default").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = builder.build_plan(&qr).unwrap();
        assert_eq!(shape, plan.shape())
    }
}

#[test]
fn test_plan_build_row_expr() {
    let cat = tpch_catalog();
    let shape = plan_shape![Row];
    for sql in vec![
        "select 1, 'abc', 'hello ' 'world', 1e20, 9223372036854775808",
        "select 1+2, 1-2, 1*2, 1/2",
        "select 1&2, 1|2, 1^2, 1<<2, 1>>2",
        "select true, false, 1 and 2, 1 or 2, 1 xor 2, 1=2, 1>=2, 1>2, 1<=2, 1<2, 1<>2",
        "select null, date '2021-01-01'",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = builder.build_plan(&qr).unwrap();
        assert_eq!(shape, plan.shape())
    }
}

#[test]
fn test_plan_build_aggr_expr() {
    let cat = j_catalog();
    let sql = "select count(*), count(distinct c0), sum(c0), max(c1), min(c1), avg(c1) from t1";
    let builder = PlanBuilder::new(Arc::clone(&cat), "j").unwrap();
    let (_, qr) = parse_query(MySQL(sql)).unwrap();
    let plan = builder.build_plan(&qr).unwrap();
    assert_eq!(plan_shape![Aggr, Proj, Table], plan.shape())
}

#[test]
fn test_plan_build_expr_from_table() {
    let cat = tpch_catalog();
    let shape = plan_shape![Proj, Proj, Table];
    for sql in vec![
        "select -l_orderkey, ~l_orderkey, not l_orderkey = 1 from lineitem",
        "select l_tax is null, l_tax is not null, l_tax is true, l_tax is not true, l_tax is false, l_tax is not false from lineitem",
        "select l_shipdate <=> l_commitdate, l_comment like 'A%', l_comment not like 'A%', l_comment regexp '.*', l_comment not regexp '.*' from lineitem",
        "select l_quantity in (1,2,3), l_quantity not in (1,2,3), l_quantity between 1 and 3, l_quantity not between 1 and 3 from lineitem",
        "select extract(year from l_shipdate), extract(quarter from l_shipdate), extract(month from l_shipdate), extract(week from l_shipdate), extract(day from l_shipdate) from lineitem",
        "select extract(hour from l_shipdate), extract(minute from l_shipdate), extract(second from l_shipdate), extract(microsecond from l_shipdate) from lineitem",
        "select substring(l_comment, 1), substring(l_comment, 1, 10) from lineitem",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = builder.build_plan(&qr).unwrap();
        assert_eq!(shape, plan.shape())
    }
}

#[test]
fn test_plan_build_select_table() {
    let cat = tpch_catalog();
    for (sql, n_cols, shape) in vec![
        ("select 1 from lineitem", 1, plan_shape![Proj, Proj, Table]),
        ("select l_orderkey from lineitem", 1, plan_shape![Proj, Proj, Table]),
        ("select l_orderkey from tpch.lineitem", 1, plan_shape![Proj, Proj, Table]),
        ("select lineitem.l_orderkey from lineitem", 1, plan_shape![Proj, Proj, Table]),
        ("select lineitem.l_orderkey from tpch.lineitem", 1, plan_shape![Proj, Proj, Table]),
        ("select tpch.lineitem.l_orderkey from lineitem", 1, plan_shape![Proj, Proj, Table]),
        ("select tpch.lineitem.l_orderkey, tpch.lineitem.l_quantity from lineitem", 2, plan_shape![Proj, Proj, Table]),
        ("select tpch.lineitem.l_orderkey from lineitem, (select 1) b", 1, plan_shape![Proj, Join, Proj, Table, Row]),
        ("select n, l_orderkey from lineitem, (select 1 as n) b", 2, plan_shape![Proj, Join, Proj, Table, Row]),
        ("select l_orderkey, l_partkey from lineitem", 2, plan_shape![Proj, Proj, Table]),
        ("select l_orderkey, l_partkey from lineitem as li", 2, plan_shape![Proj, Proj, Table]),
        ("select * from lineitem", 16, plan_shape![Proj, Proj, Table]),
        ("select lineitem.* from lineitem", 16, plan_shape![Proj, Proj, Table]),
        ("select tpch.lineitem.* from lineitem", 16, plan_shape![Proj, Proj, Table]),
        ("select l_quantity, tpch.lineitem.* from lineitem where l_quantity > 10", 17, plan_shape![Proj, Filt, Proj, Table]),
        ("select l_quantity, tpch.lineitem.* from lineitem, (select 1) b where l_quantity > 10", 17, plan_shape![Proj, Filt, Join, Proj, Table, Row]),
        ("select l_orderkey from lineitem where true", 1, plan_shape![Proj, Filt, Proj, Table]),
        ("select l_orderkey from lineitem where 1", 1, plan_shape![Proj, Filt, Proj, Table]),
        ("select l_orderkey from lineitem where l_comment is null", 1, plan_shape![Proj, Filt, Proj, Table]),
        ("select tpch.lineitem.l_orderkey from lineitem, lineitem as l2 where lineitem.l_orderkey = l2.l_orderkey", 1, plan_shape![Proj, Filt, Join, Proj, Table, Proj, Table]),
        ("select lineitem.l_orderkey from lineitem, lineitem as l2 where lineitem.l_orderkey = l2.l_orderkey", 1, plan_shape![Proj, Filt, Join, Proj, Table, Proj, Table]),
        ("select l_orderkey from lineitem group by l_orderkey", 1, plan_shape![Aggr, Proj, Table]),
        ("select l_orderkey, count(*) from lineitem group by l_orderkey", 2, plan_shape![Aggr, Proj, Table]),
        ("select count(*), l_orderkey from lineitem group by l_orderkey", 2, plan_shape![Aggr, Proj, Table]),
        ("select l_orderkey, count(*) from lineitem where l_tax > 0.1 group by l_orderkey", 2, plan_shape![Aggr, Filt, Proj, Table]),
        ("select l_orderkey from lineitem group by l_orderkey having count(*) > 1", 1, plan_shape![Filt, Aggr, Proj, Table]),
        ("select l_orderkey from lineitem group by l_orderkey having sum(l_extendedprice * l_tax) > 1", 1, plan_shape![Filt, Aggr, Proj, Table]),
        ("select l_orderkey from lineitem where l_shipdate is null group by l_orderkey having count(*) > 1", 1, plan_shape![Filt, Aggr, Filt, Proj, Table]),
        ("select l_orderkey from lineitem having l_orderkey is not null", 1, plan_shape![Filt, Proj, Proj, Table]),
        ("select count(*) from lineitem having sum(1) > 0", 1, plan_shape![Filt, Aggr, Proj, Table]),
        ("select l_orderkey, count(*) from lineitem group by l_orderkey having l_orderkey > 0", 2, plan_shape![Filt, Aggr, Proj, Table]),
        ("select l_orderkey from lineitem where l_tax > 0 having l_orderkey < 10", 1, plan_shape![Filt, Proj, Filt, Proj, Table]),
        ("select l_orderkey k, count(*) from lineitem group by l_orderkey", 2, plan_shape![Aggr, Proj, Table]),
        ("select l_orderkey from lineitem order by l_orderkey", 1, plan_shape![Sort, Proj, Proj, Table]),
        ("select l_orderkey from lineitem order by lineitem.l_orderkey", 1, plan_shape![Sort, Proj, Proj, Table]),
        ("select l_orderkey from lineitem order by tpch.lineitem.l_orderkey", 1, plan_shape![Sort, Proj, Proj, Table]),
        ("select l_orderkey from lineitem order by l_linenumber", 1, plan_shape![Sort, Proj, Proj, Table]),
        ("select l_orderkey from lineitem order by l_linenumber desc", 1, plan_shape![Sort, Proj, Proj, Table]),
        ("select l_orderkey k from lineitem order by k", 1, plan_shape![Sort, Proj, Proj, Table]),
        ("select l_orderkey k, sum(1) from lineitem group by l_orderkey order by k", 2, plan_shape![Sort, Aggr, Proj, Table]),
        ("select l_orderkey k, sum(1) from lineitem group by k order by k", 2, plan_shape![Sort, Aggr, Proj, Table]),
        ("select l_orderkey k, sum(1) from lineitem group by l_orderkey order by l_orderkey", 2, plan_shape![Sort, Aggr, Proj, Table]),
        ("select l_orderkey, count(*) from lineitem group by l_orderkey order by l_orderkey", 2, plan_shape![Sort, Aggr, Proj, Table]),
        ("select l_orderkey, sum(l_tax) from lineitem group by l_orderkey order by sum(l_tax) desc, l_orderkey", 2, plan_shape![Sort, Aggr, Proj, Table]),
        ("select count(*) from lineitem order by count(*)", 1, plan_shape![Sort, Aggr, Proj, Table]),
        ("select l_orderkey from lineitem limit 10", 1, plan_shape![Limit, Proj, Proj, Table]),
        ("select l_orderkey from lineitem limit 10 offset 10", 1, plan_shape![Limit, Proj, Proj, Table]),
        ("select l_orderkey from lineitem order by l_quantity desc limit 10", 1, plan_shape![Limit, Sort, Proj, Proj, Table]),
        ("select distinct l_orderkey from lineitem", 1, plan_shape![Aggr, Proj, Table]),
        ("select distinct l_orderkey from lineitem limit 1", 1, plan_shape![Limit, Aggr, Proj, Table]),
        ("select distinct count(*) from lineitem", 1, plan_shape![Aggr, Proj, Table]),
        ("select distinct l_orderkey from lineitem group by l_orderkey", 1, plan_shape![Aggr, Proj, Table]),
        ("select * from (select l_orderkey from lineitem order by l_orderkey limit 1) a, (select l_orderkey from lineitem order by l_orderkey desc limit 1) b", 2, plan_shape![Proj, Join, Limit, Sort, Proj, Proj, Table, Limit, Sort, Proj, Proj, Table]),
        ("select a.* from (select l_orderkey, l_linenumber from lineitem) a, (select l_orderkey from lineitem limit 1) b", 2, plan_shape![Proj, Join, Proj, Proj, Table, Limit, Proj, Proj, Table]),
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = match builder.build_plan(&qr) {
            Ok(plan) => plan,
            Err(e) => {
                eprintln!("sql={}", sql);
                panic!("{:?}", e)
            }
        };
        print_plan(sql, &plan);
        assert_eq!(shape, plan.shape());
        let p = plan.qry_set.get(&plan.root).unwrap();
        assert_eq!(n_cols, p.out_cols().len());
    }
}

#[test]
fn test_plan_build_join() {
    let cat = j_catalog();
    for (sql, shape) in vec![
        (
            "select * from t0 cross join t1",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.* from t1 cross join t2 x2",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 cross join t2",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select j.l1.* from t1 l1 cross join t2 l2",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select j.l1.c0 from t1 l1 cross join t2 l2",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.* from t1 join t2",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1, t2, t3",
            plan_shape![Proj, Join, Proj, Table, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 join t2 join t3",
            plan_shape![Proj, Join, Join, Proj, Table, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0, t2.c0 from t1 join t2 on t1.c0 = t2.c0",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 left join t2 on t1.c1 = t2.c1",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 right join t2 on t1.c1 = t2.c1",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 left join t2 right join t3",
            // because we implements conversion from right join to left join,
            // the shape is changed accordingly.
            plan_shape![Proj, Join, Proj, Table, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 full join t2 on t1.c1 = t2.c1",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 full join t2 on t1.c1 = t2.c1 and t1.c0 > 10",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 join t2 on t1.c1 = t2.c1, t3",
            plan_shape![Proj, Join, Join, Proj, Table, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1, t2 join t3 on t2.c2 = t3.c3",
            plan_shape![Proj, Join, Proj, Table, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 join t2 using (c1)",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 join t2 using (c0, c1)",
            plan_shape![Proj, Join, Proj, Table, Proj, Table],
        ),
        (
            "select t1.c0 from t1 join t2 left join t3 on t1.c1 = t2.c1 or t1.c1 = t3.c1",
            plan_shape![Proj, Join, Join, Proj, Table, Proj, Table, Proj, Table],
        ),
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "j").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = match builder.build_plan(&qr) {
            Err(e) => {
                eprintln!("sql={}", sql);
                panic!("{:?}", e)
            }
            Ok(plan) => plan,
        };
        assert_eq!(shape, plan.shape());
        print_plan(sql, &plan)
    }
}

#[test]
fn test_plan_build_subquery() {
    let cat = j_catalog();
    // success
    for sql in vec![
        "select * from t0, (select * from t1) tmp",
        "select * from t0 where c0 in (select c0 from t1)",
        "select * from t0 where c0 not in (select c0 from t1)",
        "select * from t0 where not c0 in (select c0 from t1)",
        "select * from t1 where c0 in (select c0 from t2 where t2.c1 = t1.c1)",
        "select * from t1 x where c0 in (select c0 from t2 where t2.c1 = x.c1)",
        "select * from t1 where c0 not in (select c0 from t2 tx where tx.c1 = t1.c1)",
        "select * from t1 where not c0 in (select c0 from t2 where t2.c1 = j.t1.c1)",
        "select * from t1 where exists (select c1 from t2)",
        "select * from t1 where not exists (select 1 from t2)",
        "select * from t1 where exists (select 1 from t2 where t2.c0 = t1.c0)",
        "select * from t1 where not exists (select 1 from t2 x2 where x2.c1 = t1.c1)",
        "select * from t1 where exists (select 1 from t2 where exists (select 1 from t1))",
        "select * from t1 where exists (select 1 from t2 x2 where exists (select 1 from t3 where t3.c0 = x2.c0))",
        "select * from t3 where exists (select 1 from t2 where exists (select 1 from t1 where t1.c0 = t3.c3))",
        "select * from t3 where exists (select 1 from t2 where exists (select 1 from t1 where t1.c0 = c3))",
        "select * from t3 where exists (select 1 from t2 where exists (select 1 from t1 where t1.c0 = c2))",
        "select * from t3 where exists (select 1 from t2 where exists (select 1 from t1 where c0 = t3.c0))",
        "select * from t3 where exists (select 1 from t2, (select c0 from t1 where t1.c1 = t3.c1) tt where tt.c0 = t2.c0)",
        "select * from t3 where exists (with tmp as (select 1 from t2 where t2.c0 = t3.c0) select * from tmp)",
        "with cte as (select 1 as cx) select * from cte where exists (select 1 from t1 where t1.c0 = cx)",
        "with cte (a, b) as (select 1, 2) select * from cte",
        "with cte (a, b) as (select c0, c1 from t1) select * from cte",
        "with cte (a, b) as (select c0, c1 from t1 where c0 > 0 order by c0 limit 10) select * from cte",
        "with cte (a, b) as (select t1.c0, t2.c0 from t1 join t2) select * from cte",
        "select * from t1 where c0 > (select max(c0) from t2 where t2.c1 = t1.c1)",
        "select * from t2 where c0 > (select max(c0) from t1 where t1.c1 = c2)",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "j").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = match builder.build_plan(&qr) {
            Ok(plan) => plan,
            Err(e) => {
                eprintln!("sql={}", sql);
                panic!("{:?}", e)
            }
        };
        print_plan(sql, &plan)
    }

    // fail
    for sql in vec![
        "select * from t0, (select * from t1 where t1.c0 = t0.c0) tmp",
        "select * from t0, (select * from t1 where exists (select 1 from t2 where t2.c0 = t0.c0)) tmp",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "j").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        assert!(builder.build_plan(&qr).is_err())
    }
}

#[test]
fn test_plan_build_location() {
    use Location::*;
    for (sql, expected) in vec![
        ("select 1", vec![Virtual]),
        ("select 1 from t1", vec![Intermediate, Disk]),
        (
            "select 1 from t1, (select 1) t2",
            vec![Intermediate, Disk, Virtual],
        ),
        (
            "select 1 from (select 1 from t1) t2",
            vec![Intermediate, Intermediate, Disk],
        ),
    ] {
        assert_j_plan(sql, |s, p| {
            print_plan(s, &p);
            let mut queries = vec![];
            collect_queries(&p.qry_set, p.root, &mut queries);
            let actual: Vec<_> = queries.into_iter().map(|subq| subq.location).collect();
            assert_eq!(expected, actual)
        })
    }
}

pub(crate) fn j_catalog() -> Arc<dyn QueryCatalog> {
    let mut builder = MemCatalogBuilder::default();
    builder.add_schema("j").unwrap();
    builder
        .add_table(
            "j",
            "t0",
            &vec![ColumnSpec::new("c0", DataType::I32, ColumnAttr::empty())],
        )
        .unwrap();
    builder
        .add_table(
            "j",
            "t1",
            &vec![
                ColumnSpec::new("c0", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c1", DataType::I32, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "j",
            "t2",
            &vec![
                ColumnSpec::new("c0", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c1", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c2", DataType::I32, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "j",
            "t3",
            &vec![
                ColumnSpec::new("c0", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c1", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c2", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c3", DataType::I32, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "j",
            "t4",
            &vec![
                ColumnSpec::new("c0", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c1", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c2", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c3", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c4", DataType::I32, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    let cat = builder.build();
    Arc::new(cat)
}

pub(crate) fn assert_j_plan<F: FnOnce(&str, QueryPlan)>(sql: &str, f: F) {
    let cat = j_catalog();
    let plan = build_plan(&cat, sql);
    f(sql, plan)
}

pub(crate) fn assert_j_plan1<F: FnOnce(&str, QueryPlan)>(
    cat: &Arc<dyn QueryCatalog>,
    sql: &str,
    f: F,
) {
    let plan = build_plan(&cat, sql);
    f(sql, plan)
}

pub(crate) fn assert_j_plan2<F: FnOnce(&str, QueryPlan, &str, QueryPlan)>(
    cat: &Arc<dyn QueryCatalog>,
    sql1: &str,
    sql2: &str,
    f: F,
) {
    let p1 = build_plan(&cat, sql1);
    let p2 = build_plan(&cat, sql2);
    f(sql1, p1, sql2, p2)
}

pub(crate) fn build_plan(cat: &Arc<dyn QueryCatalog>, sql: &str) -> QueryPlan {
    let builder = PlanBuilder::new(Arc::clone(&cat), "j").unwrap();
    let (_, qr) = parse_query(MySQL(sql)).unwrap();
    builder.build_plan(&qr).unwrap()
}

pub(crate) fn get_lvl_queries(plan: &QueryPlan, lvl: usize) -> Vec<&Subquery> {
    if lvl == 0 {
        return plan.root_query().into_iter().collect();
    }
    let roots = get_lvl_queries(plan, lvl - 1);
    let mut res = vec![];
    for root in roots {
        for (_, query_id) in root.scope.query_aliases.iter() {
            if let Some(subq) = plan.qry_set.get(query_id) {
                res.push(subq);
            }
        }
    }
    res
}

pub(crate) fn collect_queries<'a>(
    qs: &'a QuerySet,
    root: QueryID,
    queries: &mut Vec<&'a Subquery>,
) {
    if let Some(root) = qs.get(&root) {
        queries.push(root);
        for (_, query_id) in root.scope.query_aliases.iter() {
            collect_queries(qs, *query_id, queries)
        }
    }
}

pub(crate) fn get_filt_expr(plan: &QueryPlan) -> Vec<xngin_expr::Expr> {
    match plan.root_query() {
        Some(subq) => get_subq_filt_expr(subq),
        None => vec![],
    }
}

pub(crate) fn get_subq_filt_expr(subq: &Subquery) -> Vec<xngin_expr::Expr> {
    let mut cfe = CollectFiltExpr(vec![]);
    let _ = subq.root.walk(&mut cfe);
    cfe.0
}

pub(crate) fn get_subq_by_location<'a>(
    plan: &'a QueryPlan,
    location: Location,
) -> Vec<&'a Subquery> {
    let mut subqs = vec![];
    if let Some(subq) = plan.root_query() {
        let mut csbl = CollectSubqByLocation {
            qry_set: &plan.qry_set,
            subqs: &mut subqs,
            location,
        };
        let _ = subq.root.walk(&mut csbl);
    }
    subqs
}

struct CollectFiltExpr(Vec<xngin_expr::Expr>);

impl OpVisitor for CollectFiltExpr {
    #[inline]
    fn enter(&mut self, op: &Op) -> bool {
        match op {
            Op::Filt(filt) => {
                self.0 = filt.pred.clone();
                false
            }
            _ => true,
        }
    }
}

struct CollectSubqByLocation<'a, 'b> {
    qry_set: &'a QuerySet,
    subqs: &'b mut Vec<&'a Subquery>,
    location: Location,
}

impl<'a, 'b> OpVisitor for CollectSubqByLocation<'a, 'b> {
    #[inline]
    fn enter(&mut self, op: &Op) -> bool {
        match op {
            Op::Query(qry_id) => {
                if let Some(subq) = self.qry_set.get(qry_id) {
                    if subq.location == self.location {
                        self.subqs.push(subq);
                    }
                    let _ = subq.root.walk(self);
                }
            }
            _ => (),
        }
        true
    }
}

#[test]
fn test_plan_build_setop() {
    let cat = tpch_catalog();
    for (sql, shape) in vec![
        (
            "select * from lineitem union select * from lineitem",
            plan_shape![Setop, Proj, Proj, Table, Proj, Proj, Table],
        ),
        (
            "select * from lineitem union all select * from lineitem",
            plan_shape![Setop, Proj, Proj, Table, Proj, Proj, Table],
        ),
        ("select 1 union select 2", plan_shape![Setop, Row, Row]),
        (
            "select l_orderkey from lineitem union select 1",
            plan_shape![Setop, Proj, Proj, Table, Row],
        ),
        (
            "select 1 union select l_orderkey from lineitem",
            plan_shape![Setop, Row, Proj, Proj, Table],
        ),
        (
            "select 1 union select 2 union select 3",
            plan_shape![Setop, Setop, Row, Row, Row],
        ),
        (
            "select l_orderkey from lineitem union select 1 union select l_orderkey from lineitem",
            plan_shape![Setop, Setop, Proj, Proj, Table, Row, Proj, Proj, Table],
        ),
        (
            "select 1 union select l_orderkey from lineitem union select 1",
            plan_shape![Setop, Setop, Row, Proj, Proj, Table, Row],
        ),
        (
            "select l_orderkey from lineitem except select l_linenumber from lineitem",
            plan_shape![Setop, Proj, Proj, Table, Proj, Proj, Table],
        ),
        (
            "select l_orderkey from lineitem except all select l_linenumber from lineitem",
            plan_shape![Setop, Proj, Proj, Table, Proj, Proj, Table],
        ),
        (
            "select l_orderkey from lineitem intersect select l_linenumber from lineitem",
            plan_shape![Setop, Proj, Proj, Table, Proj, Proj, Table],
        ),
        (
            "select l_orderkey from lineitem intersect all select l_linenumber from lineitem",
            plan_shape![Setop, Proj, Proj, Table, Proj, Proj, Table],
        ),
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = builder.build_plan(&qr).unwrap();
        assert_eq!(shape, plan.shape());
        print_plan(sql, &plan)
    }
}

#[test]
fn test_plan_build_with() {
    let cat = tpch_catalog();
    for (sql, n_cols, shape) in vec![
        ("with a as (select 1) select * from a", 1, plan_shape![Proj, Row]),
        ("with a as (select 1) select 2", 1, plan_shape![Row]),
        ("with a as (select 1, 2) select * from a", 2, plan_shape![Proj, Row]),
        ("with a as (select count(*) from lineitem) select `count(*)` from a", 1, plan_shape![Proj, Aggr, Proj, Table]),
        ("with a as (select count(*) c from lineitem) select c from a", 1, plan_shape![Proj, Aggr, Proj, Table]),
        ("with a (c) as (select count(*) from lineitem) select a.c from a", 1, plan_shape![Proj, Aggr, Proj, Table]),
        ("with a (x, y) as (select l_orderkey, count(*) from lineitem group by l_orderkey) select x, y from a", 2, plan_shape![Proj, Aggr, Proj, Table]),
        ("with a as (select 1), b as (select 2) select * from a, b", 2, plan_shape![Proj, Join, Row, Row]),
        ("with a as (select 1) select * from lineitem, a", 17, plan_shape![Proj, Join, Proj, Table, Row]),
        ("with a as (select 1) select lineitem.* from lineitem, a", 16, plan_shape![Proj, Join, Proj, Table, Row]),
        ("with a as (select 1) select tpch.lineitem.* from lineitem, a", 16, plan_shape![Proj, Join, Proj, Table, Row]),
        ("with a as (select 1) select l_orderkey from lineitem, a", 1, plan_shape![Proj, Join, Proj, Table, Row]),
        ("with a as (select l_orderkey, count(*) cnt from (select * from lineitem) li group by l_orderkey) select l_orderkey from a where cnt > 10", 1, plan_shape![Proj, Filt, Aggr, Proj, Proj, Table]),
        ("with a as (select l_orderkey, n from lineitem, (select 1 as n) b) select * from a", 2, plan_shape![Proj, Proj, Join, Proj, Table, Row]),
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = builder.build_plan(&qr).unwrap();
        assert_eq!(shape, plan.shape());
        let p = plan.qry_set.get(&plan.root).unwrap();
        assert_eq!(n_cols, p.out_cols().len());
        print_plan(sql, &plan)
    }
}

#[inline]
fn empty_catalog() -> Arc<dyn QueryCatalog> {
    let mut builder = MemCatalogBuilder::default();
    builder.add_schema("default").unwrap();
    let cat = builder.build();
    Arc::new(cat)
}

#[inline]
pub(crate) fn tpch_catalog() -> Arc<dyn QueryCatalog> {
    let mut builder = MemCatalogBuilder::default();
    builder.add_schema("tpch").unwrap();
    builder
        .add_table(
            "tpch",
            "lineitem",
            &vec![
                ColumnSpec::new("l_orderkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("l_partkey", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("l_suppkey", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("l_linenumber", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("l_quantity", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("l_extendedprice", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("l_discount", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("l_tax", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("l_returnflag", DataType::Char, ColumnAttr::empty()),
                ColumnSpec::new("l_linestatus", DataType::Char, ColumnAttr::empty()),
                ColumnSpec::new("l_shipdate", DataType::Date, ColumnAttr::empty()),
                ColumnSpec::new("l_commitdate", DataType::Date, ColumnAttr::empty()),
                ColumnSpec::new("l_receiptdate", DataType::Date, ColumnAttr::empty()),
                ColumnSpec::new("l_shipinstruct", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("l_shipmode", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("l_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    let cat = builder.build();
    Arc::new(cat)
}

pub(crate) fn print_plan(sql: &str, plan: &QueryPlan) {
    use crate::explain::Explain;
    println!("SQL: {}", sql);
    let mut s = String::new();
    plan.explain(&mut s).unwrap();
    println!("Plan:\n{}", s)
}
