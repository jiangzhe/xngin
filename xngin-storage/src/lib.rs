//! The storage of X-Engine.
//!
//! Data are organized by data blocks.
//! Each block contains positional index, synopses and
//! encoded data in columnar format.
pub mod attr;
pub mod block;
pub mod codec;
pub mod error;
