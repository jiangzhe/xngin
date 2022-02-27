//! This module defines canonical rules that transform the plan
//! in early stage. "Canonical" means if the rule applies, the plan
//! is supposed to be always better, so that no cost model involved.
use crate::error::Result;
use crate::query::QueryPlan;
use bitflags::bitflags;
use xngin_expr::Effect;

pub mod col_prune;
pub mod derived_unfold;
pub mod expr_simplify;
pub mod joingraph_initialize;
pub mod op_eliminate;
pub mod outerjoin_reduce;
pub mod pred_pullup;
pub mod pred_pushdown;

pub use col_prune::col_prune;
pub use derived_unfold::derived_unfold;
pub use expr_simplify::expr_simplify;
pub use joingraph_initialize::joingraph_initialize;
pub use op_eliminate::op_eliminate;
pub use outerjoin_reduce::outerjoin_reduce;
pub use pred_pullup::pred_pullup;
pub use pred_pushdown::pred_pushdown;

bitflags! {
    pub struct RuleEffect: u8 {
        const NONE = 0x00;
        const OP = 0x01;
        const EXPR = 0x02;
        const OPEXPR = Self::OP.bits | Self::EXPR.bits;
    }
}

impl Default for RuleEffect {
    #[inline]
    fn default() -> Self {
        RuleEffect::NONE
    }
}

impl Effect for RuleEffect {
    #[inline]
    fn merge(&mut self, other: Self) {
        *self |= other
    }
}

#[inline]
pub fn rule_optimize(plan: &mut QueryPlan) -> Result<()> {
    let mut eff = init_rule_optimize(plan)?;
    for _ in 0..10 {
        match eff {
            RuleEffect::OPEXPR => {
                eff = RuleEffect::NONE;
                eff |= expr_simplify(plan)?;
                eff |= op_eliminate(plan)?;
                eff |= pred_pushdown(plan)?;
            }
            RuleEffect::OP => {
                eff = RuleEffect::NONE;
                eff |= expr_simplify(plan)?;
            }
            RuleEffect::EXPR => {
                eff = RuleEffect::NONE;
                eff |= pred_pushdown(plan)?;
                eff |= op_eliminate(plan)?;
            }
            _ => break,
        }
    }
    joingraph_initialize(plan)?;
    Ok(())
}

#[inline]
pub fn init_rule_optimize(plan: &mut QueryPlan) -> Result<RuleEffect> {
    let mut eff = RuleEffect::NONE;
    // Run column pruning as first step, to remove unused columns in operator tree.
    // this will largely reduce effort of other rules.
    eff |= col_prune(plan)?; // onetime
                             // Run expression simplify as second step, fold constants, normalize expressions.
    eff |= expr_simplify(plan)?;
    // Run operator eliminate after expression simplify, to remove unnecessary operators.
    eff |= op_eliminate(plan)?;
    // Run outerjoin reduce to update join type top down.
    eff |= outerjoin_reduce(plan)?; // onetime
                                    // Run predicate pushdown
    eff |= pred_pushdown(plan)?;
    // Run predicate pullup with predicate propagate for future predicate pushdown.
    pred_pullup(plan)?; // onetime
                        // Run predicate pushdown again
    eff |= pred_pushdown(plan)?;
    // Run column pruning again
    eff |= col_prune(plan)?;
    // unfold derived tables to gather more tables to join graph.
    eff |= derived_unfold(plan)?; // onetime
    Ok(eff)
}
