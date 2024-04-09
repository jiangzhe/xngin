mod builder;

use crate::digraph::{DiGraph, NodeIndex};
use crate::error::{Error, Result};
use crate::lgc::LgcPlan;
use builder::PhyBuilder;
use xngin_catalog::TableID;
use xngin_compute::eval::{QueryEvalPlan, TableEvalPlan};

/// PhyPlan is a directed graph transformed from LgcPlan.
/// One node can have multiple downstreams, e.g CTE node.
pub struct PhyPlan {
    pub graph: DiGraph<Phy, ()>,
    pub start: Vec<NodeIndex>,
    pub end: NodeIndex,
}

impl PhyPlan {
    #[inline]
    pub fn new(lgc: &LgcPlan) -> Result<Self> {
        // todo: currently we ignore attached plans
        let subq = lgc.root_query().ok_or(Error::EmptyPlan)?;
        PhyBuilder::new(&lgc.qry_set).build(subq)
    }
}

/// Phy is physical plan node.
pub struct Phy {
    pub kind: PhyKind,
}

pub enum PhyKind {
    TableScan(PhyTableScan),
    Proj(PhyProj),
    Row(PhyRow),
}

pub struct PhyProj {
    pub evals: QueryEvalPlan,
}

pub struct PhyRow {
    evals: QueryEvalPlan,
}

pub struct PhyTableScan {
    evals: TableEvalPlan,
    table_id: TableID,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::LgcBuilder;
    use crate::lgc::LgcPlan;
    use xngin_catalog::mem_impl::MemCatalog;
    use xngin_catalog::{Catalog, ColumnAttr, ColumnSpec, TableSpec};
    use xngin_datatype::PreciseType;
    use xngin_sql::parser::{dialect, parse_query_verbose};

    // #[test]
    // fn test_build_phy() {
    //     let cat = phy_catalog();
    //     for sql in vec!["select c0 from t0", "select c0 + 1 from t0"] {
    //         let lgc = build_lgc(&cat, sql);
    //         assert!(PhyPlan::new(&lgc).is_ok());
    //     }
    // }

    // fn phy_catalog() -> MemCatalog {
    //     let cata = MemCatalog::default();
    //     cata.create_schema("phy").unwrap();
    //     cata.create_table(TableSpec::new(
    //         "phy",
    //         "t0",
    //         vec![
    //             ColumnSpec::new("c0", PreciseType::i32(), ColumnAttr::empty()),
    //             ColumnSpec::new("c1", PreciseType::i32(), ColumnAttr::empty()),
    //         ],
    //     ))
    //     .unwrap();
    //     cata
    // }

    fn build_lgc<C: Catalog>(cat: &C, sql: &str) -> LgcPlan {
        let qe = parse_query_verbose(dialect::MySQL(sql)).unwrap();
        LgcBuilder::new(cat, "phy").unwrap().build(&qe).unwrap()
    }
}
