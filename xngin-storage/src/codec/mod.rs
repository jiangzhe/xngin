mod single;

use std::sync::Arc;

pub use single::Single;
use xngin_common::array::Array;

/// Codec of attributes.
/// The design of codec is heavily inspired by paper "Data Blocks: Hybrid OLTP
/// and OLAP on Compressed Storage using both Vectorization and Compilation".
/// The main idea is to make the codec byte-addressable, similar between
/// in-memory and persistent format.
/// Byte-addressable is to make the point search efficient, especially without
/// decompression.
/// the similar format between memory and disk enable easy persistence.
#[derive(Debug, Clone)]
pub enum Codec {
    Single(Single),
    Array(Arc<Array>),
}

impl Codec {
    /// Create a new array codec.
    #[inline]
    pub fn new_array(data: Array) -> Self {
        Codec::Array(Arc::new(data))
    }

    /// Clone self to owned.
    #[inline]
    pub fn to_owned(&self) -> Self {
        match self {
            Codec::Single(s) => Codec::Single(s.clone()),
            Codec::Array(a) => Codec::Array(Arc::new(Array::to_owned(&*a))),
        }
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        match self {
            Codec::Single(s) => s.len,
            Codec::Array(a) => a.len(),
        }
    }

    /// Returns array codec if match
    #[inline]
    pub fn as_array(&self) -> Option<&Array> {
        match self {
            Codec::Array(a) => Some(a.as_ref()),
            _ => None,
        }
    }

    /// Returns single codec if match
    #[inline]
    pub fn as_single(&self) -> Option<&Single> {
        match self {
            Codec::Single(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecKind {
    Single,
    Array,
    // Dict,
    // Trunc,
    // String,
}
