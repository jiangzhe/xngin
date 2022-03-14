use std::sync::Arc;
use xngin_catalog::mem_impl::{ColumnSpec, MemCatalogBuilder};
use xngin_catalog::{ColumnAttr, QueryCatalog};
use xngin_datatype::PreciseType;

#[inline]
pub fn tpch_catalog() -> Arc<dyn QueryCatalog> {
    let mut builder = MemCatalogBuilder::default();
    builder.add_schema("tpch").unwrap();
    builder
        .add_table(
            "tpch",
            "lineitem",
            &[
                ColumnSpec::new("l_orderkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("l_partkey", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("l_suppkey", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("l_linenumber", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new(
                    "l_quantity",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new(
                    "l_extendedprice",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new(
                    "l_discount",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("l_tax", PreciseType::decimal(18, 2), ColumnAttr::empty()),
                ColumnSpec::new("l_returnflag", PreciseType::char(1), ColumnAttr::empty()),
                ColumnSpec::new("l_linestatus", PreciseType::char(1), ColumnAttr::empty()),
                ColumnSpec::new("l_shipdate", PreciseType::date(), ColumnAttr::empty()),
                ColumnSpec::new("l_commitdate", PreciseType::date(), ColumnAttr::empty()),
                ColumnSpec::new("l_receiptdate", PreciseType::date(), ColumnAttr::empty()),
                ColumnSpec::new("l_shipinstruct", PreciseType::char(25), ColumnAttr::empty()),
                ColumnSpec::new("l_shipmode", PreciseType::char(10), ColumnAttr::empty()),
                ColumnSpec::new("l_comment", PreciseType::varchar(44), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "orders",
            &[
                ColumnSpec::new("o_orderkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("o_custkey", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("o_orderstatus", PreciseType::char(1), ColumnAttr::empty()),
                ColumnSpec::new(
                    "o_totalprice",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("o_orderdate", PreciseType::date(), ColumnAttr::empty()),
                ColumnSpec::new(
                    "o_orderpriority",
                    PreciseType::char(15),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("o_clerk", PreciseType::char(15), ColumnAttr::empty()),
                ColumnSpec::new("o_shippriority", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("o_comment", PreciseType::varchar(79), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "customer",
            &[
                ColumnSpec::new("c_custkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("c_name", PreciseType::char(25), ColumnAttr::empty()),
                ColumnSpec::new("c_address", PreciseType::varchar(40), ColumnAttr::empty()),
                ColumnSpec::new("c_nationkey", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("c_phone", PreciseType::varchar(15), ColumnAttr::empty()),
                ColumnSpec::new(
                    "c_acctbal",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("c_mktsegment", PreciseType::char(10), ColumnAttr::empty()),
                ColumnSpec::new("c_comment", PreciseType::varchar(117), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "partsupp",
            &[
                ColumnSpec::new("ps_partkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("ps_suppkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("ps_availqty", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new(
                    "ps_supplycost",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("ps_comment", PreciseType::varchar(199), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "part",
            &[
                ColumnSpec::new("p_partkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("p_name", PreciseType::char(55), ColumnAttr::empty()),
                ColumnSpec::new("p_mfgr", PreciseType::char(25), ColumnAttr::empty()),
                ColumnSpec::new("p_brand", PreciseType::char(10), ColumnAttr::empty()),
                ColumnSpec::new("p_type", PreciseType::varchar(25), ColumnAttr::empty()),
                ColumnSpec::new("p_size", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("p_container", PreciseType::char(10), ColumnAttr::empty()),
                ColumnSpec::new(
                    "p_retailprice",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("p_comment", PreciseType::varchar(23), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "supplier",
            &[
                ColumnSpec::new("s_suppkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("s_name", PreciseType::char(25), ColumnAttr::empty()),
                ColumnSpec::new("s_address", PreciseType::varchar(40), ColumnAttr::empty()),
                ColumnSpec::new("s_nationkey", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("s_phone", PreciseType::char(15), ColumnAttr::empty()),
                ColumnSpec::new(
                    "s_acctbal",
                    PreciseType::decimal(18, 2),
                    ColumnAttr::empty(),
                ),
                ColumnSpec::new("s_comment", PreciseType::varchar(101), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "nation",
            &[
                ColumnSpec::new("n_nationkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("n_name", PreciseType::char(25), ColumnAttr::empty()),
                ColumnSpec::new("n_regionkey", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("n_comment", PreciseType::varchar(152), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    builder
        .add_table(
            "tpch",
            "region",
            &[
                ColumnSpec::new("r_regionkey", PreciseType::i32(), ColumnAttr::PK),
                ColumnSpec::new("r_name", PreciseType::char(55), ColumnAttr::empty()),
                ColumnSpec::new("r_comment", PreciseType::char(152), ColumnAttr::empty()),
            ],
        )
        .unwrap();
    let cat = builder.build();
    Arc::new(cat)
}
