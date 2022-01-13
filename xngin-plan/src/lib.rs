//! This module defines all logical plans, with optimizer framework
//! and implementation.
pub mod alias;
pub mod builder;
pub mod error;
pub mod expr;
pub mod func;
pub mod id;
pub mod op;
pub mod pred;
pub mod query;
pub mod resolv;
pub mod scope;
pub mod type_infer;
