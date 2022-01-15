use std::sync::Arc;
use xngin_catalog::mem_impl::{ColumnSpec, MemCatalogBuilder};
use xngin_catalog::{ColumnAttr, QueryCatalog};
use xngin_datatype::DataType;

#[inline]
pub fn tpch_catalog() -> Arc<dyn QueryCatalog + Send + Sync> {
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
    builder
        .add_table(
            "tpch",
            "orders",
            &vec![
                ColumnSpec::new("o_orderkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("o_custkey", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("o_orderstatus", DataType::Char, ColumnAttr::empty()),
                ColumnSpec::new("o_totalprice", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("o_orderdate", DataType::Date, ColumnAttr::empty()),
                ColumnSpec::new("o_orderpriority", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("o_clerk", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("o_shippriority", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("o_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "customer",
            &vec![
                ColumnSpec::new("c_custkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("c_name", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("c_address", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("c_nationkey", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("c_phone", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("c_acctbal", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("c_mktsegment", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("c_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "partsupp",
            &vec![
                ColumnSpec::new("ps_partkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("ps_suppkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("ps_availqty", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("ps_supplycost", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("ps_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "part",
            &vec![
                ColumnSpec::new("p_partkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("p_name", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("p_mfgr", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("p_brand", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("p_type", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("p_size", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("p_container", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("p_retailprice", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("p_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "supplier",
            &vec![
                ColumnSpec::new("s_suppkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("s_name", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("s_address", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("s_nationkey", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("s_phone", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("s_acctbal", DataType::Decimal, ColumnAttr::empty()),
                ColumnSpec::new("s_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "nation",
            &vec![
                ColumnSpec::new("n_nationkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("n_name", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("n_regionkey", DataType::I32, ColumnAttr::empty()),
                ColumnSpec::new("n_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "region",
            &vec![
                ColumnSpec::new("r_regionkey", DataType::I32, ColumnAttr::PK),
                ColumnSpec::new("r_name", DataType::String, ColumnAttr::empty()),
                ColumnSpec::new("r_comment", DataType::String, ColumnAttr::empty()),
            ],
        )
        .unwrap();
    let cat = builder.build();
    Arc::new(cat)
}
