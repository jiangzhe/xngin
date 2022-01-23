//! This module defines canonical rules that transform the plan
//! in early stage. "Canonical" means if the rule applies, the plan
//! is supposed to be always better, so that no cost model involved.

pub mod col_prune;

pub use col_prune::col_prune;
