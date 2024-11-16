use doradb_catalog::mem_impl::MemCatalog;
use doradb_catalog::{Catalog, ColumnAttr, ColumnSpec, TableSpec};
use doradb_datatype::PreciseType;

#[inline]
pub fn tpch_catalog() -> MemCatalog {
    let cata = MemCatalog::default();
    cata.create_schema("tpch").unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "lineitem",
        vec![
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
            ColumnSpec::new("l_returnflag", PreciseType::ascii(1), ColumnAttr::empty()),
            ColumnSpec::new("l_linestatus", PreciseType::ascii(1), ColumnAttr::empty()),
            ColumnSpec::new("l_shipdate", PreciseType::date(), ColumnAttr::empty()),
            ColumnSpec::new("l_commitdate", PreciseType::date(), ColumnAttr::empty()),
            ColumnSpec::new("l_receiptdate", PreciseType::date(), ColumnAttr::empty()),
            ColumnSpec::new(
                "l_shipinstruct",
                PreciseType::ascii(25),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new("l_shipmode", PreciseType::ascii(10), ColumnAttr::empty()),
            ColumnSpec::new("l_comment", PreciseType::var_utf8(44), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "orders",
        vec![
            ColumnSpec::new("o_orderkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("o_custkey", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new("o_orderstatus", PreciseType::ascii(1), ColumnAttr::empty()),
            ColumnSpec::new(
                "o_totalprice",
                PreciseType::decimal(18, 2),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new("o_orderdate", PreciseType::date(), ColumnAttr::empty()),
            ColumnSpec::new(
                "o_orderpriority",
                PreciseType::ascii(15),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new("o_clerk", PreciseType::ascii(15), ColumnAttr::empty()),
            ColumnSpec::new("o_shippriority", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new("o_comment", PreciseType::var_utf8(79), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "customer",
        vec![
            ColumnSpec::new("c_custkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("c_name", PreciseType::utf8(25), ColumnAttr::empty()),
            ColumnSpec::new("c_address", PreciseType::var_utf8(40), ColumnAttr::empty()),
            ColumnSpec::new("c_nationkey", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new("c_phone", PreciseType::var_ascii(15), ColumnAttr::empty()),
            ColumnSpec::new(
                "c_acctbal",
                PreciseType::decimal(18, 2),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new("c_mktsegment", PreciseType::ascii(10), ColumnAttr::empty()),
            ColumnSpec::new("c_comment", PreciseType::var_utf8(117), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "partsupp",
        vec![
            ColumnSpec::new("ps_partkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("ps_suppkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("ps_availqty", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new(
                "ps_supplycost",
                PreciseType::decimal(18, 2),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new(
                "ps_comment",
                PreciseType::var_utf8(199),
                ColumnAttr::empty(),
            ),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "part",
        vec![
            ColumnSpec::new("p_partkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("p_name", PreciseType::ascii(55), ColumnAttr::empty()),
            ColumnSpec::new("p_mfgr", PreciseType::ascii(25), ColumnAttr::empty()),
            ColumnSpec::new("p_brand", PreciseType::ascii(10), ColumnAttr::empty()),
            ColumnSpec::new("p_type", PreciseType::var_utf8(25), ColumnAttr::empty()),
            ColumnSpec::new("p_size", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new("p_container", PreciseType::ascii(10), ColumnAttr::empty()),
            ColumnSpec::new(
                "p_retailprice",
                PreciseType::decimal(18, 2),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new("p_comment", PreciseType::var_utf8(23), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "supplier",
        vec![
            ColumnSpec::new("s_suppkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("s_name", PreciseType::utf8(25), ColumnAttr::empty()),
            ColumnSpec::new("s_address", PreciseType::var_utf8(40), ColumnAttr::empty()),
            ColumnSpec::new("s_nationkey", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new("s_phone", PreciseType::ascii(15), ColumnAttr::empty()),
            ColumnSpec::new(
                "s_acctbal",
                PreciseType::decimal(18, 2),
                ColumnAttr::empty(),
            ),
            ColumnSpec::new("s_comment", PreciseType::var_utf8(101), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "nation",
        vec![
            ColumnSpec::new("n_nationkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("n_name", PreciseType::ascii(25), ColumnAttr::empty()),
            ColumnSpec::new("n_regionkey", PreciseType::i32(), ColumnAttr::empty()),
            ColumnSpec::new("n_comment", PreciseType::var_utf8(152), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata.create_table(TableSpec::new(
        "tpch",
        "region",
        vec![
            ColumnSpec::new("r_regionkey", PreciseType::i32(), ColumnAttr::PK),
            ColumnSpec::new("r_name", PreciseType::ascii(55), ColumnAttr::empty()),
            ColumnSpec::new("r_comment", PreciseType::utf8(152), ColumnAttr::empty()),
        ],
    ))
    .unwrap();
    cata
}
