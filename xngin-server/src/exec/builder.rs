use xngin_plan::phy::{PhyPlan, Phy, PhyKind};
use xngin_plan::digraph::{DiGraph, NodeIndex};
use xngin_protocol::mysql::error::{Error, Result};
use crate::exec::ExecPlan;
// use xngin_storage::bitmap::{Bitmap, bitmap_u8s_set, bitmap_u8s_get};
use std::collections::VecDeque;

pub struct ExecBuilder<'a> {
    graph: &'a DiGraph<Phy, ()>,
    start: VecDeque<NodeIndex>,
    end: NodeIndex,
}

impl<'a> ExecBuilder<'a> {
    #[inline]
    pub fn new(phy: &'a PhyPlan) -> Self {
        ExecBuilder{
            graph: &phy.graph, 
            start: phy.start.iter().cloned().collect(),
            end: phy.end,
        }
    }

    /// Build 
    #[inline]
    pub fn build(self) -> Result<ExecPlan> {
        let Self{graph, mut start, end} = self;
        let mut nodes = VecDeque::new();
        while let Some(start_idx) = start.pop_front() {
            let phy = graph.node(start_idx)
                .ok_or(Error::InvalidExecutorState())?;
            match &phy.kind {
                PhyKind::Row(row) => {

                }
                _ => return Err(Error::InvalidExecutorState()),
            }
        }
        Ok(ExecPlan{nodes})
    }
}
