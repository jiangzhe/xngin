use std::sync::Arc;
use xngin_frontend::parser::dialect::Ansi;
use xngin_frontend::parser::parse_query_verbose;
use xngin_plan::builder::PlanBuilder;
use xngin_plan::explain::Explain;
use xngin_plan::rule::col_prune;
use xngin_tpch_tests::tpch_catalog;

macro_rules! check_tpch_col_prune {
    ( $($filename:literal),+ ) => {
        let cat = tpch_catalog();
        for sql in vec![
            $(
                include_str!($filename)
            ),*
        ] {
            let (_, qry) = parse_query_verbose(Ansi(sql)).unwrap();
            let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
            let mut plan = builder.build_plan(&qry).unwrap();
            col_prune(&mut plan).unwrap();
            let mut s = String::new();
            assert!(plan.explain(&mut s).is_ok());
            println!("Explain plan:\n{}", s)
        }
    };
}

#[test]
fn test_col_prune_tpch() {
    check_tpch_col_prune![
        "../../sql/tpch1.sql",
        "../../sql/tpch2.sql",
        "../../sql/tpch3.sql",
        "../../sql/tpch4.sql",
        "../../sql/tpch5.sql",
        "../../sql/tpch6.sql",
        "../../sql/tpch7.sql",
        "../../sql/tpch8.sql",
        "../../sql/tpch9.sql",
        "../../sql/tpch10.sql",
        "../../sql/tpch11.sql",
        "../../sql/tpch12.sql",
        "../../sql/tpch13.sql",
        "../../sql/tpch14.sql",
        "../../sql/tpch15.sql",
        "../../sql/tpch16.sql",
        "../../sql/tpch17.sql",
        "../../sql/tpch18.sql",
        "../../sql/tpch19.sql",
        "../../sql/tpch20.sql",
        "../../sql/tpch21.sql",
        "../../sql/tpch22.sql"
    ];
}
