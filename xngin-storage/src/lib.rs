//! The storage of Xngin.
//!
//! Data are organized by data blocks.
//! Each block contains positional index, synopses and
//! encoded data in columnar format.
pub mod block;
pub mod codec;
pub mod error;
// pub mod ser;
pub mod attr;
