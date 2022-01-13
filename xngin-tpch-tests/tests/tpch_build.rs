use xngin_frontend::parser::dialect::MySQL;
use xngin_frontend::parser::parse_query_verbose;
use xngin_plan::builder::PlanBuilder;
use xngin_tpch_tests::tpch_catalog;

macro_rules! check_build {
    ( $filename:literal ) => {
        let cat = tpch_catalog();
        let sql = include_str!($filename);
        let (_, qry) = parse_query_verbose(MySQL(sql)).unwrap();
        let builder = PlanBuilder::new(cat, "tpch").unwrap();
        let _ = builder.build_plan(&qry).unwrap();
    }
}
