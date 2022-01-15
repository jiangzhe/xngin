use super::*;
use std::sync::Arc;
use xngin_catalog::mem_impl::{ColumnSpec, MemCatalogBuilder};
use xngin_catalog::ColumnAttr;
use xngin_datatype::DataType;
use xngin_frontend::parser::dialect::MySQL;
use xngin_frontend::parser::parse_query;

#[test]
fn test_plan_build_select_row() {
    let cat = empty_catalog();
    for sql in vec![
        "select 1",
        "select 1, 2, 3",
        "select 1 as a",
        "select 1 as a, 2 as b",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "default").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        builder.build_plan(&qr).unwrap();
    }
}

#[test]
fn test_plan_build_expr() {
    let cat = tpch_catalog();
    for sql in vec![
        "select 1, 'abc', 'hello ' 'world', 1e20, 9223372036854775808",
        "select count(*), count(distinct l_orderkey), sum(l_quantity), max(l_discount), min(l_tax), avg(l_extendedprice) from lineitem",
        "select -l_orderkey, ~l_orderkey, not l_orderkey = 1 from lineitem",
        "select 1+2, 1-2, 1*2, 1/2",
        "select 1&2, 1|2, 1^2, 1<<2, 1>>2",
        "select true, false, 1 and 2, 1 or 2, 1 xor 2, 1=2, 1>=2, 1>2, 1<=2, 1<2, 1<>2",
        "select l_tax is null, l_tax is not null, l_tax is true, l_tax is not true, l_tax is false, l_tax is not false from lineitem",
        "select l_shipdate <=> l_commitdate, l_comment like 'A%', l_comment not like 'A%', l_comment regexp '.*', l_comment not regexp '.*' from lineitem",
        "select l_quantity in (1,2,3), l_quantity not in (1,2,3), l_quantity between 1 and 3, l_quantity not between 1 and 3 from lineitem",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        builder.build_plan(&qr).unwrap();
    }
}

#[test]
fn test_plan_build_select_table() {
    let cat = tpch_catalog();
    for (sql, n_cols) in vec![
        ("select 1 from lineitem", 1),
        ("select l_orderkey from lineitem", 1),
        ("select l_orderkey from tpch.lineitem", 1),
        ("select lineitem.l_orderkey from lineitem", 1),
        ("select lineitem.l_orderkey from tpch.lineitem", 1),
        ("select tpch.lineitem.l_orderkey from lineitem", 1),
        ("select tpch.lineitem.l_orderkey, tpch.lineitem.l_quantity from lineitem", 2),
        ("select tpch.lineitem.l_orderkey from lineitem, (select 1) b", 1),
        ("select n, l_orderkey from lineitem, (select 1 as n) b", 2),
        ("select l_orderkey, l_partkey from lineitem", 2),
        ("select l_orderkey, l_partkey from lineitem as li", 2),
        ("select * from lineitem", 16),
        ("select lineitem.* from lineitem", 16),
        ("select tpch.lineitem.* from lineitem", 16),
        ("select l_quantity, tpch.lineitem.* from lineitem where l_quantity > 10", 17),
        ("select l_quantity, tpch.lineitem.* from lineitem, (select 1) b where l_quantity > 10", 17),
        ("select l_orderkey from lineitem where true", 1),
        ("select l_orderkey from lineitem where 1", 1),
        ("select l_orderkey from lineitem where l_comment is null", 1),
        ("select tpch.lineitem.l_orderkey from lineitem, lineitem as l2 where lineitem.l_orderkey = l2.l_orderkey", 1),
        ("select lineitem.l_orderkey from lineitem, lineitem as l2 where lineitem.l_orderkey = l2.l_orderkey", 1),
        ("select l_orderkey from lineitem group by l_orderkey", 1),
        ("select l_orderkey, count(*) from lineitem group by l_orderkey", 2),
        ("select count(*), l_orderkey from lineitem group by l_orderkey", 2),
        ("select l_orderkey from lineitem group by l_orderkey having count(*) > 1", 1),
        ("select l_orderkey from lineitem having l_quantity > 10", 1),
        ("select count(*) from lineitem having sum(1) > 0", 1),
        ("select l_orderkey, count(*) from lineitem group by l_orderkey having l_orderkey > 0", 2),
        ("select l_orderkey from lineitem where l_tax > 0 having l_quantity < 10", 1),
        ("select l_orderkey k, count(*) from lineitem group by l_orderkey", 2),
        ("select l_orderkey from lineitem order by l_orderkey", 1),
        ("select l_orderkey from lineitem order by lineitem.l_orderkey", 1),
        ("select l_orderkey from lineitem order by tpch.lineitem.l_orderkey", 1),
        ("select l_orderkey from lineitem order by l_linenumber", 1),
        ("select l_orderkey from lineitem order by l_linenumber desc", 1),
        ("select l_orderkey k from lineitem order by k", 1),
        ("select l_orderkey k, sum(1) from lineitem group by l_orderkey order by k", 2),
        ("select l_orderkey k, sum(1) from lineitem group by k order by k", 2),
        ("select l_orderkey k, sum(1) from lineitem group by l_orderkey order by l_orderkey", 2),
        ("select l_orderkey, count(*) from lineitem group by l_orderkey order by l_orderkey", 2),
        ("select l_orderkey, sum(l_tax) from lineitem group by l_orderkey order by sum(l_tax) desc, l_orderkey", 2),
        ("select count(*) from lineitem order by count(*)", 1),
        ("select l_orderkey from lineitem limit 10", 1),
        ("select l_orderkey from lineitem limit 10 offset 10", 1),
        ("select l_orderkey from lineitem order by l_quantity desc limit 10", 1),
        ("select distinct l_orderkey from lineitem", 1),
        ("select distinct count(*) from lineitem", 1),
        ("select distinct l_orderkey from lineitem group by l_orderkey", 1),
        ("select * from (select l_orderkey from lineitem order by l_orderkey limit 1) a, (select l_orderkey from lineitem order by l_orderkey desc limit 1) b", 2),
        ("select a.* from (select l_orderkey, l_linenumber from lineitem) a, (select l_orderkey from lineitem limit 1) b", 2),
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
        let p = plan.queries.get(&plan.root).unwrap();
        assert_eq!(n_cols, p.scope.out_cols.len());
    }
}

#[test]
fn test_plan_build_join() {
    let cat = tpch_catalog();
    for sql in vec![
        "select * from lineitem l1 cross join lineitem l2",
        "select l1.* from lineitem l1 cross join lineitem l2",
        "select l1.l_orderkey from lineitem l1 cross join lineitem l2",
        "select tpch.l1.* from lineitem l1 cross join lineitem l2",
        "select tpch.l1.l_orderkey from lineitem l1 cross join lineitem l2",
        "select l1.* from lineitem l1 join lineitem l2",
        "select l1.l_orderkey from lineitem l1 join lineitem l2 on l1.l_orderkey = l2.l_orderkey",
        "select l1.l_orderkey from lineitem l1 left join lineitem l2 on l1.l_orderkey = l2.l_orderkey",
        "select l1.l_orderkey from lineitem l1 right join lineitem l2 on l1.l_orderkey = l2.l_orderkey",
        "select l1.l_orderkey from lineitem l1 full join lineitem l2 on l1.l_orderkey = l2.l_orderkey",
        "select l1.l_orderkey from lineitem l1 full join lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_quantity > 10",
        "select l1.l_orderkey from lineitem l1 join lineitem l2 on l1.l_orderkey = l2.l_orderkey, lineitem l3",
        "select l1.l_orderkey from lineitem l1, lineitem l2 join lineitem l3 on l2.l_orderkey = l3.l_orderkey",
        "select l1.l_orderkey from lineitem l1 join lineitem l2 using (l_orderkey)",
        "select l1.l_orderkey from lineitem l1 join lineitem l2 using (l_orderkey, l_linenumber)",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        if let Err(e) = builder.build_plan(&qr) {
            eprintln!("sql={}", sql);
            panic!("{:?}", e)
        }
    }
}

#[test]
fn test_plan_build_subquery() {
    let cat = tpch_catalog();
    for sql in vec![
        "select * from lineitem where l_orderkey in (select l_orderkey from lineitem)",
        "select * from lineitem where l_orderkey not in (select l_orderkey from lineitem)",
        "select * from lineitem where not l_orderkey in (select l_orderkey from lineitem)",
        "select * from lineitem where l_orderkey in (select l_orderkey from lineitem l2 where l2.l_linenumber = lineitem.l_linenumber)",
        "select * from lineitem l1 where l_orderkey not in (select l_orderkey from lineitem l2 where l2.l_linenumber = l1.l_linenumber)",
        "select * from lineitem where not l_orderkey in (select l_orderkey from lineitem l2 where l2.l_linenumber = tpch.lineitem.l_linenumber)",
        "select * from lineitem where exists (select l_orderkey from lineitem)",
        "select * from lineitem where not exists (select 1 from lineitem)",
        "select * from lineitem where exists (select 1 from lineitem l2 where l2.l_orderkey = lineitem.l_orderkey)",
        "select * from lineitem where not exists (select 1 from lineitem l2 where l2.l_orderkey = lineitem.l_orderkey)",
        "select * from lineitem where exists (select 1 from lineitem l2 where exists (select 1 from lineitem))",
        "select * from lineitem where exists (select 1 from lineitem l2 where exists (select 1 from lineitem l3 where l3.l_orderkey = l2.l_orderkey))",
        "select * from lineitem where exists (select 1 from lineitem l2 where exists (select 1 from lineitem l3 where l3.l_orderkey = lineitem.l_orderkey))",
        "select * from lineitem l1 where exists (select 1 from lineitem l2 where exists (select 1 from lineitem l3 where l3.l_orderkey = l1.l_orderkey))",
        "select * from lineitem l1 where exists (select 1 from lineitem l2, (select l_orderkey from lineitem l3 where l3.l_orderkey = l1.l_orderkey) tt where tt.l_orderkey = l2.l_orderkey)",
        "select * from lineitem where exists (with tmp as (select 1 from lineitem l2 where l2.l_orderkey = lineitem.l_orderkey) select * from tmp)",
        "with cte as (select 1 as c0) select * from cte where exists (select 1 from lineitem where lineitem.l_orderkey = c0)",
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        if let Err(e) = builder.build_plan(&qr) {
            eprintln!("sql={}", sql);
            panic!("{:?}", e)
        }
    }
}

#[test]
fn test_plan_build_with() {
    let cat = tpch_catalog();
    for (sql, n_cols) in vec![
        ("with a as (select 1) select * from a", 1),
        ("with a as (select 1) select 2", 1),
        ("with a as (select 1, 2) select * from a", 2),
        ("with a as (select count(*) from lineitem) select `count(*)` from a", 1),
        ("with a as (select count(*) c from lineitem) select c from a", 1),
        ("with a (c) as (select count(*) from lineitem) select a.c from a", 1),
        ("with a (x, y) as (select l_orderkey, count(*) from lineitem group by l_orderkey) select x, y from a", 2),
        ("with a as (select 1), b as (select 2) select * from a, b", 2),
        ("with a as (select 1) select * from lineitem, a", 17),
        ("with a as (select 1) select lineitem.* from lineitem, a", 16),
        ("with a as (select 1) select tpch.lineitem.* from lineitem, a", 16),
        ("with a as (select 1) select l_orderkey from lineitem, a", 1),
        ("with a as (select l_orderkey, count(*) cnt from (select * from lineitem) li group by l_orderkey) select l_orderkey from a where cnt > 10", 1),
        ("with a as (select l_orderkey, n from lineitem, (select 1 as n) b) select * from a", 2),
    ] {
        let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
        let (_, qr) = parse_query(MySQL(sql)).unwrap();
        let plan = builder.build_plan(&qr).unwrap();
        let p = plan.queries.get(&plan.root).unwrap();
        assert_eq!(n_cols, p.scope.out_cols.len());
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
fn tpch_catalog() -> Arc<dyn QueryCatalog> {
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
