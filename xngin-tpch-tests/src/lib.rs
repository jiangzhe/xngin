use std::sync::Arc;
use xngin_catalog::mem_impl::{ColumnSpec, MemCatalogBuilder};
use xngin_catalog::{ColumnAttr, DataType, QueryCatalog};

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
    let cat = builder.build();
    Arc::new(cat)
}
