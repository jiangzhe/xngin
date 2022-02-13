//! This module defines canonical rules that transform the plan
//! in early stage. "Canonical" means if the rule applies, the plan
//! is supposed to be always better, so that no cost model involved.

pub mod col_prune;
pub mod expr_simplify;
pub mod op_eliminate;

pub use col_prune::col_prune;
pub use expr_simplify::expr_simplify;
