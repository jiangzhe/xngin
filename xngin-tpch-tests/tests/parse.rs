use std::ops::{Add, Div, Mul, Sub};
use xngin_frontend::ast::*;
use xngin_frontend::parser::dialect::Ansi;
use xngin_frontend::parser::parse_query_verbose;

macro_rules! col {
    ( $($lit:literal).* ) => {
        Expr::column_ref(vec![ $( ($lit).into() ),* ])
    }
}

macro_rules! auto_derived {
    ( $lit:literal ) => {
        DerivedCol::auto_alias(Expr::column_ref(vec![$lit.into()]), $lit)
    };
}

macro_rules! table {
    ( $name:literal as $alias:literal ) => {
        TableRef::primitive(TablePrimitive::Named(($name).into(), Some($alias.into())))
    };
    ( $name:literal ) => {
        TableRef::primitive(TablePrimitive::Named(($name).into(), None))
    };
}

macro_rules! check_sql {
    ($expected:ident, $filename:literal) => {
        let sql = include_str!($filename).trim();
        let res = match parse_query_verbose(Ansi(sql)) {
            Ok(query) => query,
            Err(err) => {
                eprintln!("Failed to parse query:\n{}", err);
                panic!()
            }
        };
        assert_eq!($expected, res);
    };
}

#[test]
fn parse_tpch1() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                DerivedCol::auto_alias(col!("l_returnflag"), "l_returnflag"),
                DerivedCol::auto_alias(col!("l_linestatus"), "l_linestatus"),
                DerivedCol::new(Expr::sum(col!("l_quantity")), "sum_qty".into()),
                DerivedCol::new(Expr::sum(col!("l_extendedprice")), "sum_base_price".into()),
                DerivedCol::new(
                    Expr::sum(Expr::mul(
                        col!("l_extendedprice"),
                        Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                    )),
                    "sum_disc_price".into(),
                ),
                DerivedCol::new(
                    Expr::sum(Expr::mul(
                        Expr::mul(
                            col!("l_extendedprice"),
                            Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                        ),
                        Expr::add(Expr::numeric_lit("1"), col!("l_tax")),
                    )),
                    "sum_charge".into(),
                ),
                DerivedCol::new(Expr::avg(col!("l_quantity")), "avg_qty".into()),
                DerivedCol::new(Expr::avg(col!("l_extendedprice")), "avg_price".into()),
                DerivedCol::new(Expr::avg(col!("l_discount")), "avg_disc".into()),
                DerivedCol::new(Expr::count_asterisk(), "count_order".into()),
            ],
            from: vec![table!("lineitem")],
            filter: Some(Expr::cmp_le(
                col!("l_shipdate"),
                Expr::sub(
                    Expr::date_lit("1998-12-01"),
                    Expr::interval_lit("90", DatetimeUnit::Day),
                ),
            )),
            group_by: vec![col!("l_returnflag"), col!("l_linestatus")],
            having: None,
            order_by: vec![
                OrderElement::asc(col!("l_returnflag")),
                OrderElement::asc(col!("l_linestatus")),
            ],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch1.sql");
}

#[test]
fn parse_tpch2() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("s_acctbal"),
                auto_derived!("s_name"),
                auto_derived!("n_name"),
                auto_derived!("p_partkey"),
                auto_derived!("p_mfgr"),
                auto_derived!("s_address"),
                auto_derived!("s_phone"),
                auto_derived!("s_comment"),
            ],
            from: vec![
                table!("part"),
                table!("supplier"),
                table!("partsupp"),
                table!("nation"),
                table!("region"),
            ],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("p_partkey"), col!("ps_partkey")),
                Expr::cmp_eq(col!("s_suppkey"), col!("ps_suppkey")),
                Expr::cmp_eq(col!("p_size"), Expr::numeric_lit("15")),
                Expr::pred_like(col!("p_type"), Expr::charstr_lit("%BRASS".into())),
                Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                Expr::cmp_eq(col!("n_regionkey"), col!("r_regionkey")),
                Expr::cmp_eq(col!("r_name"), Expr::charstr_lit("EUROPE".into())),
                Expr::cmp_eq(
                    col!("ps_supplycost"),
                    Expr::scalar_subquery(QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols: vec![DerivedCol::auto_alias(
                                Expr::min(col!("ps_supplycost")),
                                "min(ps_supplycost)",
                            )],
                            from: vec![
                                table!("partsupp"),
                                table!("supplier"),
                                table!("nation"),
                                table!("region"),
                            ],
                            filter: Some(Expr::pred_conj(vec![
                                Expr::cmp_eq(col!("p_partkey"), col!("ps_partkey")),
                                Expr::cmp_eq(col!("s_suppkey"), col!("ps_suppkey")),
                                Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                                Expr::cmp_eq(col!("n_regionkey"), col!("r_regionkey")),
                                Expr::cmp_eq(col!("r_name"), Expr::charstr_lit("EUROPE".into())),
                            ])),
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                        }),
                    }),
                ),
            ])),
            group_by: vec![],
            having: None,
            order_by: vec![
                OrderElement::desc(col!("s_acctbal")),
                OrderElement::asc(col!("n_name")),
                OrderElement::asc(col!("s_name")),
                OrderElement::asc(col!("p_partkey")),
            ],
            limit: Some(Limit {
                limit: 100,
                offset: None,
            }),
        }),
    };
    check_sql!(expected, "../../sql/tpch2.sql");
}

#[test]
fn parse_tpch3() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("l_orderkey"),
                DerivedCol::new(
                    Expr::sum(Expr::mul(
                        col!("l_extendedprice"),
                        Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                    )),
                    "revenue".into(),
                ),
                auto_derived!("o_orderdate"),
                auto_derived!("o_shippriority"),
            ],
            from: vec![table!("customer"), table!("orders"), table!("lineitem")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("c_mktsegment"), Expr::charstr_lit("BUILDING".into())),
                Expr::cmp_eq(col!("c_custkey"), col!("o_custkey")),
                Expr::cmp_eq(col!("l_orderkey"), col!("o_orderkey")),
                Expr::cmp_lt(col!("o_orderdate"), Expr::date_lit("1995-03-15")),
                Expr::cmp_gt(col!("l_shipdate"), Expr::date_lit("1995-03-15")),
            ])),
            group_by: vec![
                col!("l_orderkey"),
                col!("o_orderdate"),
                col!("o_shippriority"),
            ],
            having: None,
            order_by: vec![
                OrderElement::desc(col!("revenue")),
                OrderElement::asc(col!("o_orderdate")),
            ],
            limit: Some(Limit {
                limit: 10,
                offset: None,
            }),
        }),
    };
    check_sql!(expected, "../../sql/tpch3.sql");
}

#[test]
fn parse_tpch4() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("o_orderpriority"),
                DerivedCol::new(Expr::count_asterisk(), "order_count".into()),
            ],
            from: vec![table!("orders")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_ge(col!("o_orderdate"), Expr::date_lit("1993-07-01")),
                Expr::cmp_lt(col!("o_orderdate"), Expr::date_lit("1993-10-01")),
                Expr::exists(QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![DerivedCol::Asterisk(vec![])],
                        from: vec![table!("lineitem")],
                        filter: Some(Expr::logical_and(
                            Expr::cmp_eq(col!("l_orderkey"), col!("o_orderkey")),
                            Expr::cmp_lt(col!("l_commitdate"), col!("l_receiptdate")),
                        )),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                }),
            ])),
            group_by: vec![col!("o_orderpriority")],
            having: None,
            order_by: vec![OrderElement::asc(col!("o_orderpriority"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch4.sql");
}

#[test]
fn parse_tpch5() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("n_name"),
                DerivedCol::new(
                    Expr::sum(Expr::mul(
                        col!("l_extendedprice"),
                        Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                    )),
                    "revenue".into(),
                ),
            ],
            from: vec![
                table!("customer"),
                table!("orders"),
                table!("lineitem"),
                table!("supplier"),
                table!("nation"),
                table!("region"),
            ],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("c_custkey"), col!("o_custkey")),
                Expr::cmp_eq(col!("l_orderkey"), col!("o_orderkey")),
                Expr::cmp_eq(col!("l_suppkey"), col!("s_suppkey")),
                Expr::cmp_eq(col!("c_nationkey"), col!("s_nationkey")),
                Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                Expr::cmp_eq(col!("n_regionkey"), col!("r_regionkey")),
                Expr::cmp_eq(col!("r_name"), Expr::charstr_lit("ASIA".into())),
                Expr::cmp_ge(col!("o_orderdate"), Expr::date_lit("1994-01-01")),
                Expr::cmp_lt(col!("o_orderdate"), Expr::date_lit("1995-01-01")),
            ])),
            group_by: vec![col!("n_name")],
            having: None,
            order_by: vec![OrderElement::desc(col!("revenue"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch5.sql");
}

#[test]
fn parse_tpch6() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![DerivedCol::new(
                Expr::sum(Expr::mul(col!("l_extendedprice"), col!("l_discount"))),
                "revenue".into(),
            )],
            from: vec![table!("lineitem")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_ge(col!("l_shipdate"), Expr::date_lit("1994-01-01")),
                Expr::cmp_lt(col!("l_shipdate"), Expr::date_lit("1995-01-01")),
                Expr::pred_btw(
                    col!("l_discount"),
                    Expr::sub(Expr::numeric_lit("0.06"), Expr::numeric_lit("0.01")),
                    Expr::add(Expr::numeric_lit("0.06"), Expr::numeric_lit("0.01")),
                ),
                Expr::cmp_lt(col!("l_quantity"), Expr::numeric_lit("24")),
            ])),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch6.sql");
}

#[test]
fn parse_tpch7() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("supp_nation"),
                auto_derived!("cust_nation"),
                auto_derived!("l_year"),
                DerivedCol::new(Expr::sum(col!("volume")), "revenue".into()),
            ],
            from: vec![TableRef::primitive(TablePrimitive::derived(
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![
                            DerivedCol::new(col!("n1"."n_name"), "supp_nation".into()),
                            DerivedCol::new(col!("n2"."n_name"), "cust_nation".into()),
                            DerivedCol::new(
                                Expr::Builtin(Builtin::Extract(
                                    DatetimeUnit::Year,
                                    Box::new(col!("l_shipdate")),
                                )),
                                "l_year".into(),
                            ),
                            DerivedCol::new(
                                Expr::mul(
                                    col!("l_extendedprice"),
                                    Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                                ),
                                "volume".into(),
                            ),
                        ],
                        from: vec![
                            table!("supplier"),
                            table!("lineitem"),
                            table!("orders"),
                            table!("customer"),
                            table!("nation" as "n1"),
                            table!("nation" as "n2"),
                        ],
                        filter: Some(Expr::pred_conj(vec![
                            Expr::cmp_eq(col!("s_suppkey"), col!("l_suppkey")),
                            Expr::cmp_eq(col!("o_orderkey"), col!("l_orderkey")),
                            Expr::cmp_eq(col!("c_custkey"), col!("o_custkey")),
                            Expr::cmp_eq(col!("s_nationkey"), col!("n1"."n_nationkey")),
                            Expr::cmp_eq(col!("c_nationkey"), col!("n2"."n_nationkey")),
                            Expr::logical_or(
                                Expr::logical_and(
                                    Expr::cmp_eq(
                                        col!("n1"."n_name"),
                                        Expr::charstr_lit("FRANCE".into()),
                                    ),
                                    Expr::cmp_eq(
                                        col!("n2"."n_name"),
                                        Expr::charstr_lit("GERMANY".into()),
                                    ),
                                ),
                                Expr::logical_and(
                                    Expr::cmp_eq(
                                        col!("n1"."n_name"),
                                        Expr::charstr_lit("GERMANY".into()),
                                    ),
                                    Expr::cmp_eq(
                                        col!("n2"."n_name"),
                                        Expr::charstr_lit("FRANCE".into()),
                                    ),
                                ),
                            ),
                            Expr::pred_btw(
                                col!("l_shipdate"),
                                Expr::date_lit("1995-01-01"),
                                Expr::date_lit("1996-12-31"),
                            ),
                        ])),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
                "shipping".into(),
            ))],
            filter: None,
            group_by: vec![col!("supp_nation"), col!("cust_nation"), col!("l_year")],
            having: None,
            order_by: vec![
                OrderElement::asc(col!("supp_nation")),
                OrderElement::asc(col!("cust_nation")),
                OrderElement::asc(col!("l_year")),
            ],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch7.sql");
}

#[test]
fn parse_tpch8() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("o_year"),
                DerivedCol::new(
                    Expr::div(
                        Expr::sum(Expr::case_when(
                            None,
                            vec![(
                                Expr::cmp_eq(col!("nation"), Expr::charstr_lit("BRAZIL".into())),
                                col!("volume"),
                            )],
                            Some(Expr::numeric_lit("0")),
                        )),
                        Expr::sum(col!("volume")),
                    ),
                    "mkt_share".into(),
                ),
            ],
            from: vec![TableRef::primitive(TablePrimitive::derived(
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![
                            DerivedCol::new(
                                Expr::Builtin(Builtin::Extract(
                                    DatetimeUnit::Year,
                                    Box::new(col!("o_orderdate")),
                                )),
                                "o_year".into(),
                            ),
                            DerivedCol::new(
                                Expr::mul(
                                    col!("l_extendedprice"),
                                    Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                                ),
                                "volume".into(),
                            ),
                            DerivedCol::new(col!("n2"."n_name"), "nation".into()),
                        ],
                        from: vec![
                            table!("part"),
                            table!("supplier"),
                            table!("lineitem"),
                            table!("orders"),
                            table!("customer"),
                            table!("nation" as "n1"),
                            table!("nation" as "n2"),
                            table!("region"),
                        ],
                        filter: Some(Expr::pred_conj(vec![
                            Expr::cmp_eq(col!("p_partkey"), col!("l_partkey")),
                            Expr::cmp_eq(col!("s_suppkey"), col!("l_suppkey")),
                            Expr::cmp_eq(col!("l_orderkey"), col!("o_orderkey")),
                            Expr::cmp_eq(col!("o_custkey"), col!("c_custkey")),
                            Expr::cmp_eq(col!("c_nationkey"), col!("n1"."n_nationkey")),
                            Expr::cmp_eq(col!("n1"."n_regionkey"), col!("r_regionkey")),
                            Expr::cmp_eq(col!("r_name"), Expr::charstr_lit("AMERICA".into())),
                            Expr::cmp_eq(col!("s_nationkey"), col!("n2"."n_nationkey")),
                            Expr::pred_btw(
                                col!("o_orderdate"),
                                Expr::date_lit("1995-01-01"),
                                Expr::date_lit("1996-12-31"),
                            ),
                            Expr::cmp_eq(
                                col!("p_type"),
                                Expr::charstr_lit("ECONOMY ANODIZED STEEL".into()),
                            ),
                        ])),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
                "all_nations".into(),
            ))],
            filter: None,
            group_by: vec![col!("o_year")],
            having: None,
            order_by: vec![OrderElement::asc(col!("o_year"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch8.sql");
}

#[test]
fn parse_tpch9() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("nation"),
                auto_derived!("o_year"),
                DerivedCol::new(Expr::sum(col!("amount")), "sum_profit".into()),
            ],
            from: vec![TableRef::primitive(TablePrimitive::derived(
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![
                            DerivedCol::new(col!("n_name"), "nation".into()),
                            DerivedCol::new(
                                Expr::builtin(Builtin::Extract(
                                    DatetimeUnit::Year,
                                    Box::new(col!("o_orderdate")),
                                )),
                                "o_year".into(),
                            ),
                            DerivedCol::new(
                                Expr::sub(
                                    Expr::mul(
                                        col!("l_extendedprice"),
                                        Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                                    ),
                                    Expr::mul(col!("ps_supplycost"), col!("l_quantity")),
                                ),
                                "amount".into(),
                            ),
                        ],
                        from: vec![
                            table!("part"),
                            table!("supplier"),
                            table!("lineitem"),
                            table!("partsupp"),
                            table!("orders"),
                            table!("nation"),
                        ],
                        filter: Some(Expr::pred_conj(vec![
                            Expr::cmp_eq(col!("s_suppkey"), col!("l_suppkey")),
                            Expr::cmp_eq(col!("ps_suppkey"), col!("l_suppkey")),
                            Expr::cmp_eq(col!("ps_partkey"), col!("l_partkey")),
                            Expr::cmp_eq(col!("p_partkey"), col!("l_partkey")),
                            Expr::cmp_eq(col!("o_orderkey"), col!("l_orderkey")),
                            Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                            Expr::pred_like(col!("p_name"), Expr::charstr_lit("%green%".into())),
                        ])),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
                "profit".into(),
            ))],
            filter: None,
            group_by: vec![col!("nation"), col!("o_year")],
            having: None,
            order_by: vec![
                OrderElement::asc(col!("nation")),
                OrderElement::desc(col!("o_year")),
            ],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch9.sql");
}

#[test]
fn parse_tpch10() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("c_custkey"),
                auto_derived!("c_name"),
                DerivedCol::new(
                    Expr::sum(Expr::mul(
                        col!("l_extendedprice"),
                        Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                    )),
                    "revenue".into(),
                ),
                auto_derived!("c_acctbal"),
                auto_derived!("n_name"),
                auto_derived!("c_address"),
                auto_derived!("c_phone"),
                auto_derived!("c_comment"),
            ],
            from: vec![
                table!("customer"),
                table!("orders"),
                table!("lineitem"),
                table!("nation"),
            ],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("c_custkey"), col!("o_custkey")),
                Expr::cmp_eq(col!("l_orderkey"), col!("o_orderkey")),
                Expr::cmp_ge(col!("o_orderdate"), Expr::date_lit("1993-10-01")),
                Expr::cmp_lt(col!("o_orderdate"), Expr::date_lit("1994-01-01")),
                Expr::cmp_eq(col!("l_returnflag"), Expr::charstr_lit("R".into())),
                Expr::cmp_eq(col!("c_nationkey"), col!("n_nationkey")),
            ])),
            group_by: vec![
                col!("c_custkey"),
                col!("c_name"),
                col!("c_acctbal"),
                col!("c_phone"),
                col!("n_name"),
                col!("c_address"),
                col!("c_comment"),
            ],
            having: None,
            order_by: vec![OrderElement::desc(col!("revenue"))],
            limit: Some(Limit {
                limit: 20,
                offset: None,
            }),
        }),
    };
    check_sql!(expected, "../../sql/tpch10.sql");
}

#[test]
fn parse_tpch11() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("ps_partkey"),
                DerivedCol::new(
                    Expr::sum(Expr::mul(col!("ps_supplycost"), col!("ps_availqty"))),
                    Ident::Delimited("value"),
                ),
            ],
            from: vec![table!("partsupp"), table!("supplier"), table!("nation")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("ps_suppkey"), col!("s_suppkey")),
                Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                Expr::cmp_eq(col!("n_name"), Expr::charstr_lit("GERMANY".into())),
            ])),
            group_by: vec![col!("ps_partkey")],
            having: Some(Expr::cmp_gt(
                Expr::sum(Expr::mul(col!("ps_supplycost"), col!("ps_availqty"))),
                Expr::scalar_subquery(QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![DerivedCol::auto_alias(
                            Expr::mul(
                                Expr::sum(Expr::mul(col!("ps_supplycost"), col!("ps_availqty"))),
                                Expr::numeric_lit("0.0001"),
                            ),
                            "sum(ps_supplycost * ps_availqty) * 0.0001",
                        )],
                        from: vec![table!("partsupp"), table!("supplier"), table!("nation")],
                        filter: Some(Expr::pred_conj(vec![
                            Expr::cmp_eq(col!("ps_suppkey"), col!("s_suppkey")),
                            Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                            Expr::cmp_eq(col!("n_name"), Expr::charstr_lit("GERMANY".into())),
                        ])),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                }),
            )),
            order_by: vec![OrderElement::desc(Expr::column_ref(vec![
                Ident::Delimited("value"),
            ]))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch11.sql");
}

#[test]
fn parse_tpch12() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("l_shipmode"),
                DerivedCol::new(
                    Expr::sum(Expr::case_when(
                        None,
                        vec![(
                            Expr::pred_disj(vec![
                                Expr::cmp_eq(
                                    col!("o_orderpriority"),
                                    Expr::charstr_lit("1-URGENT".into()),
                                ),
                                Expr::cmp_eq(
                                    col!("o_orderpriority"),
                                    Expr::charstr_lit("2-HIGH".into()),
                                ),
                            ]),
                            Expr::numeric_lit("1"),
                        )],
                        Some(Expr::numeric_lit("0")),
                    )),
                    "high_line_count".into(),
                ),
                DerivedCol::new(
                    Expr::sum(Expr::case_when(
                        None,
                        vec![(
                            Expr::pred_conj(vec![
                                Expr::cmp_ne(
                                    col!("o_orderpriority"),
                                    Expr::charstr_lit("1-URGENT".into()),
                                ),
                                Expr::cmp_ne(
                                    col!("o_orderpriority"),
                                    Expr::charstr_lit("2-HIGH".into()),
                                ),
                            ]),
                            Expr::numeric_lit("1"),
                        )],
                        Some(Expr::numeric_lit("0")),
                    )),
                    "low_line_count".into(),
                ),
            ],
            from: vec![table!("orders"), table!("lineitem")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("o_orderkey"), col!("l_orderkey")),
                Expr::pred_in_values(
                    col!("l_shipmode"),
                    vec![
                        Expr::charstr_lit("MAIL".into()),
                        Expr::charstr_lit("SHIP".into()),
                    ],
                ),
                Expr::cmp_lt(col!("l_commitdate"), col!("l_receiptdate")),
                Expr::cmp_lt(col!("l_shipdate"), col!("l_commitdate")),
                Expr::cmp_ge(col!("l_receiptdate"), Expr::date_lit("1994-01-01")),
                Expr::cmp_lt(col!("l_receiptdate"), Expr::date_lit("1995-01-01")),
            ])),
            group_by: vec![col!("l_shipmode")],
            having: None,
            order_by: vec![OrderElement::asc(col!("l_shipmode"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch12.sql");
}

#[test]
fn parse_tpch13() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("c_count"),
                DerivedCol::new(Expr::count_asterisk(), "custdist".into()),
            ],
            from: vec![TableRef::primitive(TablePrimitive::derived(
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![
                            auto_derived!("c_custkey"),
                            DerivedCol::new(Expr::count(col!("o_orderkey")), "c_count".into()),
                        ],
                        from: vec![TableRef::joined(TableJoin::qualified(
                            table!("customer"),
                            TablePrimitive::Named("orders".into(), None),
                            JoinType::Left,
                            Some(JoinCondition::Conds(Expr::logical_and(
                                Expr::cmp_eq(col!("c_custkey"), col!("o_custkey")),
                                Expr::pred_nlike(
                                    col!("o_comment"),
                                    Expr::charstr_lit("%special%requests%".into()),
                                ),
                            ))),
                        ))],
                        filter: None,
                        group_by: vec![col!("c_custkey")],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
                "c_orders".into(),
            ))],
            filter: None,
            group_by: vec![col!("c_count")],
            having: None,
            order_by: vec![
                OrderElement::desc(col!("custdist")),
                OrderElement::desc(col!("c_count")),
            ],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch13.sql");
}

#[test]
fn parse_tpch14() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![DerivedCol::new(
                Expr::div(
                    Expr::mul(
                        Expr::numeric_lit("100.00"),
                        Expr::sum(Expr::case_when(
                            None,
                            vec![(
                                Expr::pred_like(col!("p_type"), Expr::charstr_lit("PROMO%".into())),
                                Expr::mul(
                                    col!("l_extendedprice"),
                                    Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                                ),
                            )],
                            Some(Expr::numeric_lit("0")),
                        )),
                    ),
                    Expr::sum(Expr::mul(
                        col!("l_extendedprice"),
                        Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                    )),
                ),
                "promo_revenue".into(),
            )],
            from: vec![table!("lineitem"), table!("part")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("l_partkey"), col!("p_partkey")),
                Expr::cmp_ge(col!("l_shipdate"), Expr::date_lit("1995-09-01")),
                Expr::cmp_lt(col!("l_shipdate"), Expr::date_lit("1995-10-01")),
            ])),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch14.sql");
}

#[test]
fn parse_tpch15() {
    let expected = QueryExpr {
        with: Some(With {
            recursive: false,
            elements: vec![WithElement {
                name: "revenue".into(),
                cols: vec![],
                query_expr: QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![
                            DerivedCol::new(col!("l_suppkey"), "supplier_no".into()),
                            DerivedCol::new(
                                Expr::sum(Expr::mul(
                                    col!("l_extendedprice"),
                                    Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                                )),
                                "total_revenue".into(),
                            ),
                        ],
                        from: vec![table!("lineitem")],
                        filter: Some(Expr::logical_and(
                            Expr::cmp_ge(col!("l_shipdate"), Expr::date_lit("1996-01-01")),
                            Expr::cmp_lt(col!("l_shipdate"), Expr::date_lit("1996-04-01")),
                        )),
                        group_by: vec![col!("l_suppkey")],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
            }],
        }),
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("s_suppkey"),
                auto_derived!("s_name"),
                auto_derived!("s_address"),
                auto_derived!("s_phone"),
                auto_derived!("total_revenue"),
            ],
            from: vec![table!("supplier"), table!("revenue")],
            filter: Some(Expr::logical_and(
                Expr::cmp_eq(col!("s_suppkey"), col!("supplier_no")),
                Expr::cmp_eq(
                    col!("total_revenue"),
                    Expr::scalar_subquery(QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols: vec![DerivedCol::auto_alias(
                                Expr::max(col!("total_revenue")),
                                "max(total_revenue)",
                            )],
                            from: vec![table!("revenue")],
                            filter: None,
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                        }),
                    }),
                ),
            )),
            group_by: vec![],
            having: None,
            order_by: vec![OrderElement::asc(col!("s_suppkey"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch15.sql");
}

#[test]
fn parse_tpch16() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("p_brand"),
                auto_derived!("p_type"),
                auto_derived!("p_size"),
                DerivedCol::new(
                    Expr::count_distinct(col!("ps_suppkey")),
                    "supplier_cnt".into(),
                ),
            ],
            from: vec![table!("partsupp"), table!("part")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("p_partkey"), col!("ps_partkey")),
                Expr::cmp_ne(col!("p_brand"), Expr::charstr_lit("Brand#45".into())),
                Expr::pred_nlike(col!("p_type"), Expr::charstr_lit("MEDIUM POLISHED%".into())),
                Expr::pred_in_values(
                    col!("p_size"),
                    vec![
                        Expr::numeric_lit("49"),
                        Expr::numeric_lit("14"),
                        Expr::numeric_lit("23"),
                        Expr::numeric_lit("45"),
                        Expr::numeric_lit("19"),
                        Expr::numeric_lit("3"),
                        Expr::numeric_lit("36"),
                        Expr::numeric_lit("9"),
                    ],
                ),
                Expr::pred_nin_subquery(
                    col!("ps_suppkey"),
                    QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols: vec![auto_derived!("s_suppkey")],
                            from: vec![table!("supplier")],
                            filter: Some(Expr::pred_like(
                                col!("s_comment"),
                                Expr::charstr_lit("%Customer%Complaints%".into()),
                            )),
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                        }),
                    },
                ),
            ])),
            group_by: vec![col!("p_brand"), col!("p_type"), col!("p_size")],
            having: None,
            order_by: vec![
                OrderElement::desc(col!("supplier_cnt")),
                OrderElement::asc(col!("p_brand")),
                OrderElement::asc(col!("p_type")),
                OrderElement::asc(col!("p_size")),
            ],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch16.sql");
}

#[test]
fn parse_tpch17() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![DerivedCol::new(
                Expr::div(Expr::sum(col!("l_extendedprice")), Expr::numeric_lit("7.0")),
                "avg_yearly".into(),
            )],
            from: vec![table!("lineitem"), table!("part")],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("p_partkey"), col!("l_partkey")),
                Expr::cmp_eq(col!("p_brand"), Expr::charstr_lit("Brand#23".into())),
                Expr::cmp_eq(col!("p_container"), Expr::charstr_lit("MED BOX".into())),
                Expr::cmp_lt(
                    col!("l_quantity"),
                    Expr::scalar_subquery(QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols: vec![DerivedCol::auto_alias(
                                Expr::mul(Expr::numeric_lit("0.2"), Expr::avg(col!("l_quantity"))),
                                "0.2 * avg(l_quantity)",
                            )],
                            from: vec![table!("lineitem")],
                            filter: Some(Expr::cmp_eq(col!("l_partkey"), col!("p_partkey"))),
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                        }),
                    }),
                ),
            ])),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch17.sql");
}

#[test]
fn parse_tpch18() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("c_name"),
                auto_derived!("c_custkey"),
                auto_derived!("o_orderkey"),
                auto_derived!("o_orderdate"),
                auto_derived!("o_totalprice"),
                DerivedCol::auto_alias(Expr::sum(col!("l_quantity")), "sum(l_quantity)"),
            ],
            from: vec![table!("customer"), table!("orders"), table!("lineitem")],
            filter: Some(Expr::pred_conj(vec![
                Expr::pred_in_subquery(
                    col!("o_orderkey"),
                    QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols: vec![auto_derived!("l_orderkey")],
                            from: vec![table!("lineitem")],
                            filter: None,
                            group_by: vec![col!("l_orderkey")],
                            having: Some(Expr::cmp_gt(
                                Expr::sum(col!("l_quantity")),
                                Expr::numeric_lit("300"),
                            )),
                            order_by: vec![],
                            limit: None,
                        }),
                    },
                ),
                Expr::cmp_eq(col!("c_custkey"), col!("o_custkey")),
                Expr::cmp_eq(col!("o_orderkey"), col!("l_orderkey")),
            ])),
            group_by: vec![
                col!("c_name"),
                col!("c_custkey"),
                col!("o_orderkey"),
                col!("o_orderdate"),
                col!("o_totalprice"),
            ],
            having: None,
            order_by: vec![
                OrderElement::desc(col!("o_totalprice")),
                OrderElement::asc(col!("o_orderdate")),
            ],
            limit: Some(Limit {
                limit: 100,
                offset: None,
            }),
        }),
    };
    check_sql!(expected, "../../sql/tpch18.sql");
}

#[test]
fn parse_tpch19() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![DerivedCol::new(
                Expr::sum(Expr::mul(
                    col!("l_extendedprice"),
                    Expr::sub(Expr::numeric_lit("1"), col!("l_discount")),
                )),
                "revenue".into(),
            )],
            from: vec![table!("lineitem"), table!("part")],
            filter: Some(Expr::pred_disj(vec![
                Expr::pred_conj(vec![
                    Expr::cmp_eq(col!("p_partkey"), col!("l_partkey")),
                    Expr::cmp_eq(col!("p_brand"), Expr::charstr_lit("Brand#12".into())),
                    Expr::pred_in_values(
                        col!("p_container"),
                        vec![
                            Expr::charstr_lit("SM CASE".into()),
                            Expr::charstr_lit("SM BOX".into()),
                            Expr::charstr_lit("SM PACK".into()),
                            Expr::charstr_lit("SM PKG".into()),
                        ],
                    ),
                    Expr::cmp_ge(col!("l_quantity"), Expr::numeric_lit("1")),
                    Expr::cmp_le(
                        col!("l_quantity"),
                        Expr::add(Expr::numeric_lit("1"), Expr::numeric_lit("10")),
                    ),
                    Expr::pred_btw(
                        col!("p_size"),
                        Expr::numeric_lit("1"),
                        Expr::numeric_lit("5"),
                    ),
                    Expr::pred_in_values(
                        col!("l_shipmode"),
                        vec![
                            Expr::charstr_lit("AIR".into()),
                            Expr::charstr_lit("AIR REG".into()),
                        ],
                    ),
                    Expr::cmp_eq(
                        col!("l_shipinstruct"),
                        Expr::charstr_lit("DELIVER IN PERSON".into()),
                    ),
                ]),
                Expr::pred_conj(vec![
                    Expr::cmp_eq(col!("p_partkey"), col!("l_partkey")),
                    Expr::cmp_eq(col!("p_brand"), Expr::charstr_lit("Brand#23".into())),
                    Expr::pred_in_values(
                        col!("p_container"),
                        vec![
                            Expr::charstr_lit("MED BAG".into()),
                            Expr::charstr_lit("MED BOX".into()),
                            Expr::charstr_lit("MED PKG".into()),
                            Expr::charstr_lit("MED PACK".into()),
                        ],
                    ),
                    Expr::cmp_ge(col!("l_quantity"), Expr::numeric_lit("10")),
                    Expr::cmp_le(
                        col!("l_quantity"),
                        Expr::add(Expr::numeric_lit("10"), Expr::numeric_lit("10")),
                    ),
                    Expr::pred_btw(
                        col!("p_size"),
                        Expr::numeric_lit("1"),
                        Expr::numeric_lit("10"),
                    ),
                    Expr::pred_in_values(
                        col!("l_shipmode"),
                        vec![
                            Expr::charstr_lit("AIR".into()),
                            Expr::charstr_lit("AIR REG".into()),
                        ],
                    ),
                    Expr::cmp_eq(
                        col!("l_shipinstruct"),
                        Expr::charstr_lit("DELIVER IN PERSON".into()),
                    ),
                ]),
                Expr::pred_conj(vec![
                    Expr::cmp_eq(col!("p_partkey"), col!("l_partkey")),
                    Expr::cmp_eq(col!("p_brand"), Expr::charstr_lit("Brand#34".into())),
                    Expr::pred_in_values(
                        col!("p_container"),
                        vec![
                            Expr::charstr_lit("LG CASE".into()),
                            Expr::charstr_lit("LG BOX".into()),
                            Expr::charstr_lit("LG PACK".into()),
                            Expr::charstr_lit("LG PKG".into()),
                        ],
                    ),
                    Expr::cmp_ge(col!("l_quantity"), Expr::numeric_lit("20")),
                    Expr::cmp_le(
                        col!("l_quantity"),
                        Expr::add(Expr::numeric_lit("20"), Expr::numeric_lit("10")),
                    ),
                    Expr::pred_btw(
                        col!("p_size"),
                        Expr::numeric_lit("1"),
                        Expr::numeric_lit("15"),
                    ),
                    Expr::pred_in_values(
                        col!("l_shipmode"),
                        vec![
                            Expr::charstr_lit("AIR".into()),
                            Expr::charstr_lit("AIR REG".into()),
                        ],
                    ),
                    Expr::cmp_eq(
                        col!("l_shipinstruct"),
                        Expr::charstr_lit("DELIVER IN PERSON".into()),
                    ),
                ]),
            ])),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch19.sql");
}

#[test]
fn parse_tpch20() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![auto_derived!("s_name"), auto_derived!("s_address")],
            from: vec![table!("supplier"), table!("nation")],
            filter: Some(Expr::pred_conj(vec![
                Expr::pred_in_subquery(
                    col!("s_suppkey"),
                    QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols: vec![auto_derived!("ps_suppkey")],
                            from: vec![table!("partsupp")],
                            filter: Some(Expr::logical_and(
                                Expr::pred_in_subquery(
                                    col!("ps_partkey"),
                                    QueryExpr {
                                        with: None,
                                        query: Query::table(SelectTable {
                                            q: SetQuantifier::All,
                                            cols: vec![auto_derived!("p_partkey")],
                                            from: vec![table!("part")],
                                            filter: Some(Expr::pred_like(
                                                col!("p_name"),
                                                Expr::charstr_lit("forest%".into()),
                                            )),
                                            group_by: vec![],
                                            having: None,
                                            order_by: vec![],
                                            limit: None,
                                        }),
                                    },
                                ),
                                Expr::cmp_gt(
                                    col!("ps_availqty"),
                                    Expr::scalar_subquery(QueryExpr {
                                        with: None,
                                        query: Query::table(SelectTable {
                                            q: SetQuantifier::All,
                                            cols: vec![DerivedCol::auto_alias(
                                                Expr::mul(
                                                    Expr::numeric_lit("0.5"),
                                                    Expr::sum(col!("l_quantity")),
                                                ),
                                                "0.5 * sum(l_quantity)",
                                            )],
                                            from: vec![table!("lineitem")],
                                            filter: Some(Expr::pred_conj(vec![
                                                Expr::cmp_eq(col!("l_partkey"), col!("ps_partkey")),
                                                Expr::cmp_eq(col!("l_suppkey"), col!("ps_suppkey")),
                                                Expr::cmp_ge(
                                                    col!("l_shipdate"),
                                                    Expr::date_lit("1994-01-01"),
                                                ),
                                                Expr::cmp_lt(
                                                    col!("l_shipdate"),
                                                    Expr::date_lit("1995-01-01"),
                                                ),
                                            ])),
                                            group_by: vec![],
                                            having: None,
                                            order_by: vec![],
                                            limit: None,
                                        }),
                                    }),
                                ),
                            )),
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                        }),
                    },
                ),
                Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                Expr::cmp_eq(col!("n_name"), Expr::charstr_lit("CANADA".into())),
            ])),
            group_by: vec![],
            having: None,
            order_by: vec![OrderElement::asc(col!("s_name"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch20.sql");
}

#[test]
fn parse_tpch21() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("s_name"),
                DerivedCol::new(Expr::count_asterisk(), "numwait".into()),
            ],
            from: vec![
                table!("supplier"),
                table!("lineitem" as "l1"),
                table!("orders"),
                table!("nation"),
            ],
            filter: Some(Expr::pred_conj(vec![
                Expr::cmp_eq(col!("s_suppkey"), col!("l1"."l_suppkey")),
                Expr::cmp_eq(col!("o_orderkey"), col!("l1"."l_orderkey")),
                Expr::cmp_eq(col!("o_orderstatus"), Expr::charstr_lit("F".into())),
                Expr::cmp_gt(col!("l1"."l_receiptdate"), col!("l1"."l_commitdate")),
                Expr::exists(QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![DerivedCol::Asterisk(vec![])],
                        from: vec![table!("lineitem" as "l2")],
                        filter: Some(Expr::logical_and(
                            Expr::cmp_eq(col!("l2"."l_orderkey"), col!("l1"."l_orderkey")),
                            Expr::cmp_ne(col!("l2"."l_suppkey"), col!("l1"."l_suppkey")),
                        )),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                }),
                Expr::logical_not(Expr::exists(QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![DerivedCol::Asterisk(vec![])],
                        from: vec![table!("lineitem" as "l3")],
                        filter: Some(Expr::pred_conj(vec![
                            Expr::cmp_eq(col!("l3"."l_orderkey"), col!("l1"."l_orderkey")),
                            Expr::cmp_ne(col!("l3"."l_suppkey"), col!("l1"."l_suppkey")),
                            Expr::cmp_gt(col!("l3"."l_receiptdate"), col!("l3"."l_commitdate")),
                        ])),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                })),
                Expr::cmp_eq(col!("s_nationkey"), col!("n_nationkey")),
                Expr::cmp_eq(col!("n_name"), Expr::charstr_lit("SAUDI ARABIA".into())),
            ])),
            group_by: vec![col!("s_name")],
            having: None,
            order_by: vec![
                OrderElement::desc(col!("numwait")),
                OrderElement::asc(col!("s_name")),
            ],
            limit: Some(Limit {
                limit: 100,
                offset: None,
            }),
        }),
    };
    check_sql!(expected, "../../sql/tpch21.sql");
}

#[test]
fn parse_tpch22() {
    let expected = QueryExpr {
        with: None,
        query: Query::table(SelectTable {
            q: SetQuantifier::All,
            cols: vec![
                auto_derived!("cntrycode"),
                DerivedCol::new(Expr::count_asterisk(), "numcust".into()),
                DerivedCol::new(Expr::sum(col!("c_acctbal")), "totacctbal".into()),
            ],
            from: vec![TableRef::primitive(TablePrimitive::derived(
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![
                            DerivedCol::new(
                                Expr::builtin(Builtin::Substring(
                                    Box::new(col!("c_phone")),
                                    Box::new(Expr::numeric_lit("1")),
                                    Some(Box::new(Expr::numeric_lit("2"))),
                                )),
                                "cntrycode".into(),
                            ),
                            auto_derived!("c_acctbal"),
                        ],
                        from: vec![table!("customer")],
                        filter: Some(Expr::pred_conj(vec![
                            Expr::pred_in_values(
                                Expr::builtin(Builtin::Substring(
                                    Box::new(col!("c_phone")),
                                    Box::new(Expr::numeric_lit("1")),
                                    Some(Box::new(Expr::numeric_lit("2"))),
                                )),
                                vec![
                                    Expr::charstr_lit("13".into()),
                                    Expr::charstr_lit("31".into()),
                                    Expr::charstr_lit("23".into()),
                                    Expr::charstr_lit("29".into()),
                                    Expr::charstr_lit("30".into()),
                                    Expr::charstr_lit("18".into()),
                                    Expr::charstr_lit("17".into()),
                                ],
                            ),
                            Expr::cmp_gt(
                                col!("c_acctbal"),
                                Expr::scalar_subquery(QueryExpr {
                                    with: None,
                                    query: Query::table(SelectTable {
                                        q: SetQuantifier::All,
                                        cols: vec![DerivedCol::auto_alias(
                                            Expr::avg(col!("c_acctbal")),
                                            "avg(c_acctbal)",
                                        )],
                                        from: vec![table!("customer")],
                                        filter: Some(Expr::logical_and(
                                            Expr::cmp_gt(
                                                col!("c_acctbal"),
                                                Expr::numeric_lit("0.00"),
                                            ),
                                            Expr::pred_in_values(
                                                Expr::builtin(Builtin::Substring(
                                                    Box::new(col!("c_phone")),
                                                    Box::new(Expr::numeric_lit("1")),
                                                    Some(Box::new(Expr::numeric_lit("2"))),
                                                )),
                                                vec![
                                                    Expr::charstr_lit("13".into()),
                                                    Expr::charstr_lit("31".into()),
                                                    Expr::charstr_lit("23".into()),
                                                    Expr::charstr_lit("29".into()),
                                                    Expr::charstr_lit("30".into()),
                                                    Expr::charstr_lit("18".into()),
                                                    Expr::charstr_lit("17".into()),
                                                ],
                                            ),
                                        )),
                                        group_by: vec![],
                                        having: None,
                                        order_by: vec![],
                                        limit: None,
                                    }),
                                }),
                            ),
                            Expr::logical_not(Expr::exists(QueryExpr {
                                with: None,
                                query: Query::table(SelectTable {
                                    q: SetQuantifier::All,
                                    cols: vec![DerivedCol::Asterisk(vec![])],
                                    from: vec![table!("orders")],
                                    filter: Some(Expr::cmp_eq(
                                        col!("o_custkey"),
                                        col!("c_custkey"),
                                    )),
                                    group_by: vec![],
                                    having: None,
                                    order_by: vec![],
                                    limit: None,
                                }),
                            })),
                        ])),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
                "custsale".into(),
            ))],
            filter: None,
            group_by: vec![col!("cntrycode")],
            having: None,
            order_by: vec![OrderElement::asc(col!("cntrycode"))],
            limit: None,
        }),
    };
    check_sql!(expected, "../../sql/tpch22.sql");
}
