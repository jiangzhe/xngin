use crate::digraph::{DiGraph, NodeIndex};
use crate::error::{Error, Result};
use xngin_catalog::TableID;
use xngin_compute::eval::{QueryEvalPlan, TableEvalPlan};
use xngin_plan::lgc::LgcPlan;
use xngin_plan::op::Op;
use xngin_plan::query::{Location, QuerySet, Subquery};

#[allow(dead_code)]
pub struct PhyPlan {
    graph: DiGraph<Phy, ()>,
    start: Vec<NodeIndex>,
    end: NodeIndex,
}

impl PhyPlan {
    #[inline]
    pub fn new(lgc: &LgcPlan) -> Result<Self> {
        // todo: currently we ignore attached plans
        let subq = lgc.root_query().ok_or(Error::EmptyLgcPlan)?;
        let mut builder = Builder::new(&lgc.qry_set);
        let end_point = builder.build_subquery(subq)?;
        Ok(builder.build(end_point))
    }
}

/// Phy is physical plan node.
pub struct Phy {
    pub kind: PhyKind,
}

pub enum PhyKind {
    TableScan {
        eval_plan: TableEvalPlan,
        table_id: TableID,
    },
    Project(QueryEvalPlan),
}

struct Builder<'a> {
    graph: DiGraph<Phy, ()>,
    start: Vec<NodeIndex>,
    qry_set: &'a QuerySet,
}

impl<'a> Builder<'a> {
    #[inline]
    fn new(qry_set: &'a QuerySet) -> Self {
        Builder {
            graph: DiGraph::new(),
            start: vec![],
            qry_set,
        }
    }

    #[inline]
    fn build(self, end: NodeIndex) -> PhyPlan {
        PhyPlan {
            graph: self.graph,
            start: self.start,
            end,
        }
    }

    #[inline]
    fn build_subquery(&mut self, subq: &Subquery) -> Result<NodeIndex> {
        match subq.location {
            Location::Disk => self.build_disk_scan(&subq.root),
            Location::Intermediate => self.build_intermediate(&subq.root),
            Location::Virtual => todo!(),
            Location::Memory | Location::Network => todo!(),
        }
    }

    // recursively build the intermediate data flow
    #[inline]
    fn build_intermediate(&mut self, root: &Op) -> Result<NodeIndex> {
        match root {
            Op::Proj { cols, input } => {
                let input_idx = self.build_intermediate(input)?;
                let eval_plan = QueryEvalPlan::new(cols.iter().map(|(e, _)| e))?;
                let proj = Phy {
                    kind: PhyKind::Project(eval_plan),
                };
                let curr_idx = self.graph.add_node(proj);
                self.graph.add_edge(input_idx, curr_idx, ());
                Ok(curr_idx)
            }
            Op::Query(qry_id) => {
                let subq = self.qry_set.get(qry_id).ok_or(Error::InvalidLgcNode)?;
                self.build_subquery(subq)
            }
            _ => todo!(),
        }
    }

    // Table scan supports Proj and Table operators.
    #[inline]
    fn build_disk_scan(&mut self, mut root: &Op) -> Result<NodeIndex> {
        let mut proj = None;
        let table_id;
        loop {
            match root {
                Op::Proj { cols, input } => {
                    let eval_plan = TableEvalPlan::new(cols.iter().map(|(e, _)| e))?;
                    proj = Some(eval_plan);
                    root = &**input;
                }
                Op::Table(_, tid) => {
                    table_id = Some(*tid);
                    break;
                }
                _ => return Err(Error::UnsupportedPhyTableScan),
            }
        }
        let (eval_plan, table_id) = proj.zip(table_id).ok_or(Error::UnsupportedPhyTableScan)?;
        let node = Phy {
            kind: PhyKind::TableScan {
                eval_plan,
                table_id,
            },
        };
        let idx = self.graph.add_node(node);
        self.start.push(idx);
        Ok(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xngin_catalog::mem_impl::{ColumnSpec, MemCatalog, MemCatalogBuilder};
    use xngin_catalog::{ColumnAttr, QueryCatalog};
    use xngin_datatype::PreciseType;
    use xngin_frontend::parser::{dialect, parse_query_verbose};
    use xngin_plan::builder::PlanBuilder;
    use xngin_plan::lgc::LgcPlan;

    #[test]
    fn test_build_phy() {
        let cat = phy_catalog();
        for sql in vec!["select c0 from t0", "select c0 + 1 from t0"] {
            let lgc = build_lgc(&cat, sql);
            assert!(PhyPlan::new(&lgc).is_ok());
        }
    }

    fn phy_catalog() -> MemCatalog {
        let mut builder = MemCatalogBuilder::default();
        builder.add_schema("phy").unwrap();
        builder
            .add_table(
                "phy",
                "t0",
                &vec![
                    ColumnSpec::new("c0", PreciseType::i32(), ColumnAttr::empty()),
                    ColumnSpec::new("c1", PreciseType::i32(), ColumnAttr::empty()),
                ],
            )
            .unwrap();
        builder.build()
    }

    fn build_lgc<C: QueryCatalog>(cat: &C, sql: &str) -> LgcPlan {
        let qe = parse_query_verbose(dialect::MySQL(sql)).unwrap();
        PlanBuilder::new(cat, "phy")
            .unwrap()
            .build_plan(&qe)
            .unwrap()
    }
}
